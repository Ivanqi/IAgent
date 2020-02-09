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

/**
 * Class LogProcess
 *
 * @since 2.0
 *
 * @Process(workerId={0,1,2,3,4})
 */
class LogProcess implements ProcessInterface
{
    const MAX_NUMS = 100;
    const INDEX_KEY = 'project_id';
    const KEY_RECORDS = 'records';
    private static $queueName;
    private static $faileQueueName;
    private static $maxTimeout;
    private static $client;
    private static $receiverKey;
    private static $receiverIp;
    private static $receiverPort;
    private static $errorTag = 1;
    private static $successTag = 0;

    public function __construct()
    {
        self::$queueName = config('logjob.queue_name');
        self::$faileQueueName = config('logjob.faile_queue_name');
        self::$maxTimeout = config('logjob.queue_max_timeout');
        self::$receiverKey = config('tcp.receiver_key');
        self::$receiverIp = config('tcp.receiver_ip');
        self::$receiverPort = config('tcp.receiver_port');
    }
    /**
     * @param Pool $pool
     * @param int  $workerId
     */
    public function run(Pool $pool, int $workerId): void
    { 
        while (true) {
            $this->logHandle();
            Coroutine::sleep(0.1);
        }
    }

    private function logHandle(): void
    {
        try {
            $len = Redis::LLEN(self::$queueName);
            if ($len <= 0) return;
            else if ($len > self::MAX_NUMS){
                $len = self::MAX_NUMS;
            }

            $faileArr = [];
            $logDatatArr = [];

            for ($i = 0; $i < $len; $i++) {
                $logData = Redis::BRPOPLPUSH(self::$queueName, self::$faileQueueName, self::$maxTimeout);
                if (!$logData) {
                    continue;
                }
                $faileArr[] = $logData;
                $data = json_decode($logData, true);
                if (!isset($data[self::INDEX_KEY])) {
                    continue;
                }
                $indexKey = $data[self::INDEX_KEY];
                if (!isset($logDatatArr[$indexKey])) {
                    $logDatatArr[$indexKey] = [];
                    $logDatatArr[$indexKey][self::KEY_RECORDS] = $data[self::KEY_RECORDS];
                } else {
                    $logDatatArr[$indexKey][self::KEY_RECORDS] = array_merge($logDatatArr[$indexKey][self::KEY_RECORDS], $data[self::KEY_RECORDS]);
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
                throw new TcpClientException('连接失败');
            }

            $ret = self::$client->send($logDatatArr);
            unset($logDatatArr);
            if (!$ret) {
                throw new TcpClientException('数据发送失败');
            }
            $msg = self::$client->recv(1024);
            if ($msg['code'] == self::$successTag) {
                for ($i = 0; $i < $len; $i++) {
                    Redis::lrem(self::$faileQueueName, $faileArr[$i]);
                }
                unset($faileArr);
            } else {
                throw new TcpClientException($msg['msg']);
            }

        } catch(\TcpClientException $e) {
            CLog::error("无法发送数据:". $e->getMessage());
        }
    }
}