<?php declare(strict_types=1);
/**
 * This file is part of Swoft.
 *
 * @link     https://swoft.org
 * @document https://swoft.org/docs
 * @contact  group@swoft.org
 * @license  https://github.com/swoft-cloud/swoft/blob/master/LICENSE
 */

namespace App\Process;

use Swoft\Log\Helper\CLog;
use Swoft\Log\Helper\Log;
use Swoft\Process\Annotation\Mapping\Process;
use Swoft\Process\Contract\ProcessInterface;
use Swoole\Coroutine;
use Swoole\Process\Pool;
use Swoft\Redis\Redis;
use LogSdk\TcpClient;
use LogSdk\Exception\TcpClientException;
use Swoft\Stdlib\Helper\ArrayHelper;

/**
 * Class LogProcess
 *
 * @since 2.0
 *
 * @Process(workerId={0,1,2,3,4,5})
 */
class LogProcess implements ProcessInterface
{
    private static $queueName;
    private static $faileQueueName;
    private static $maxTimeout;
    private static $client;
    private static $receiverKey;
    private static $receiverIp;
    private static $receiverPort;
    private static $errorTag = 1;
    private static $successTag = 0;
    private static $multiConsumeSwitch = false;
    private static $multiIndexKey = '';
    private static $multiKeyRecords = '';
    private static $multiMaxNums = 0;
    private static $multiMaxTimes = 0;

    public function __construct()
    {
        self::$queueName = config('logjob.queue_name');
        self::$faileQueueName = config('logjob.faile_queue_name');
        self::$maxTimeout = config('logjob.queue_max_timeout');
        self::$receiverKey = config('tcp.receiver_key');
        self::$receiverIp = config('tcp.receiver_ip');
        self::$receiverPort = config('tcp.receiver_port');
        self::$multiConsumeSwitch = config('tcp.multi_consume_switch');
        self::$multiIndexKey = config('tcp.multi_index_key');
        self::$multiKeyRecords = config('tcp.multi_key_records');
        self::$multiMaxNums = config('tcp.multi_max_nums');
        self::$multiMaxNums = config('tcp.multi_max_times');
    }
    /**
     * @param Pool $pool
     * @param int  $workerId
     */
    public function run(Pool $pool, int $workerId): void
    { 
        while (true) {
            if (self::$multiConsumeSwitch) {
                for ($i = 0; $i < self::$multiMaxNums; $i++) {
                    $this->multiConsumeFunc();
                }
            } else {
                $this->singleConsumeFunc();
            }
            Coroutine::sleep(0.1);
        }
    }

    private function singleConsumeFunc(): void
    {
        try {
            $logData = Redis::BRPOPLPUSH(self::$queueName, self::$faileQueueName, self::$maxTimeout);
            if (!$logData) return;
            // 接入LogSDK,把数据发往ICollector
            if (self::$client == NULL) {
                self::$client = new TcpClient(self::$receiverKey);
            }

            if (!self::$client->connect(self::$receiverIp, (int) self::$receiverPort)) {
                throw new TcpClientException(TcpClientException::CONNECT_FAILED);
            }
            $logDatatArr = $this->dataHandle($logData);
            $ret = self::$client->send($logDatatArr);
           
            if (!$ret) {
                throw new TcpClientException(TcpClientException::DATA_SEND_FAILED);
            }
            $msg = self::$client->recv(1024);
            if ($msg['code'] == self::$successTag) {
                Redis::lrem(self::$faileQueueName, $logData);
                unset($logDatatArr);
                unset($logData);
            } else {
                throw new TcpClientException($msg['msg']);
            }
        } catch(\TcpClientException $e) {
            CLog::error("无法发送数据:". $e->getMessage());
        }
    }

    private function multiConsumeFunc() : void
    {
        try {
            $len = Redis::LLEN(self::$queueName);

            if ($len <= 0) return;
            else if ($len > self::$multiMaxNums){
                $len = self::$multiMaxNums;
            }

            $faileArr = [];
            $logDatatArr = [];
            $num = 0;
            for ($i = 0; $i < $len; $i++) {
                $logData = Redis::BRPOPLPUSH(self::$queueName, self::$faileQueueName, self::$maxTimeout);
                if (!$logData) {
                    continue;
                }
                $faileArr[] = $logData;
                $data = json_decode($logData, true);
                if (!isset($data[self::$multiIndexKey])) {
                    continue;
                }
                $indexKey = $data[self::$multiIndexKey];
                if (!isset($logDatatArr[$indexKey])) {
                    $logDatatArr[$indexKey] = [];       
                }
                foreach ($data[self::$multiKeyRecords] as $recordName => $record) {
                    if (!isset($logDatatArr[$indexKey][$recordName])) {
                        $logDatatArr[$indexKey][$recordName] = $record;
                    } else {
                        $num++;
                        $logDatatArr[$indexKey][$recordName] = ArrayHelper::merge($logDatatArr[$indexKey][$recordName], $record);
                    }
                }
                unset($data);
                unset($logData);
            }
            if (empty($logDatatArr)) {
                return;
            }

            // 接入LogSDK,把数据发往ICollector
            if (self::$client == NULL) {
                self::$client = new TcpClient(self::$receiverKey);
            }

            if (!self::$client->connect(self::$receiverIp, (int) self::$receiverPort)) {
                throw new TcpClientException(TcpClientException::CONNECT_FAILED);
            }

            $ret = self::$client->send($logDatatArr);
            unset($logDatatArr);
            if (!$ret) {
                throw new TcpClientException(TcpClientException::DATA_SEND_FAILED);
            }

            $msg = self::$client->recv(1024);
            if ($msg['code'] == self::$successTag) {
                foreach($faileArr as $failLog) {
                    Redis::lrem(self::$faileQueueName, $failLog);
                }
                unset($faileArr);
            } else {
                throw new TcpClientException($msg['msg']);
            }
        } catch(\TcpClientException $e) {
            CLog::error("无法发送数据:". $e->getMessage());
        }
    }

    private function dataHandle(string $data): array
    {
        $data = json_decode($data, true);
        $tmpData = [];
        $tmpData[$data[self::$multiIndexKey]] = $data[self::$multiKeyRecords];
        unset($data);
        return $tmpData;
    }
}