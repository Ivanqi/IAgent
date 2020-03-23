<?php declare(strict_types=1);

namespace App\ProcessRepositories;
use Swoft\Redis\Redis;
use Swoft\Log\Helper\CLog;
use Swoft\Stdlib\Helper\ArrayHelper;
use LogSdk\TcpClient;
use LogSdk\Exception\TcpClientException;
use LogSdk\Protocol\Protocol;

class  LogProcessRepositories {

    private static $_instance;
    private static $queueName;
    private static $faileQueueName;
    private static $maxTimeout;
    private static $client;
    private static $receiverKey;
    private static $receiverIp;
    private static $receiverPort;
    private static $errorTag = 1;
    private static $successTag = 0;
    private static $multiIndexKey = '';
    private static $multiKeyRecords = '';
    private static $multiMaxNums = 0;

    public function __construct()
    {
        self::$queueName = config('logjob.queue_name');
        self::$faileQueueName = config('logjob.faile_queue_name');
        self::$maxTimeout = config('logjob.queue_max_timeout');
        self::$receiverKey = config('tcp.receiver_key');
        self::$receiverIp = config('tcp.receiver_ip');
        self::$receiverPort = config('tcp.receiver_port');
        self::$multiIndexKey = config('tcp.multi_index_key');
        self::$multiKeyRecords = config('tcp.multi_key_records');
        self::$multiMaxNums = config('tcp.multi_max_nums');
    }

    public static function getInstance()
    {
        if (!self::$_instance) {
            self::$_instance = new self();
        }
        return self::$_instance;
    }

    public function singleConsumeFunc(): void
    {
        try {
            $logData = Redis::BRPOPLPUSH(self::$queueName, self::$faileQueueName, self::$maxTimeout);
            if (!$logData) return;
            // 接入LogSDK,把数据发往ICollector
            if (self::$client == NULL) {
                self::$client = new TcpClient(self::$receiverKey, Protocol::SWOFT_PHP_PROTOCOL);
            }

            if (!self::$client->connect(self::$receiverIp, (int) self::$receiverPort, true)) {
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

    public function multiConsumeFunc() : void
    {
        try {

            $faileArr = [];
            $logDatatArr = [];
            $num = 0;
            for ($i = 0; $i < self::$multiMaxNums; $i++) {
                $logData = Redis::BRPOPLPUSH(self::$queueName, self::$faileQueueName, self::$maxTimeout);
                if (!$logData) {
                    continue;
                }
                $faileArr[] = $logData;
                $data = unserialize($logData);
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
                self::$client = new TcpClient(self::$receiverKey, Protocol::SWOFT_PHP_PROTOCOL);
            }

            if (!self::$client->connect(self::$receiverIp, (int) self::$receiverPort, true)) {
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
        $data = unserialize($data);
        $tmpData = [];
        $tmpData[$data[self::$multiIndexKey]] = $data[self::$multiKeyRecords];
        unset($data);
        return $tmpData;
    }
}