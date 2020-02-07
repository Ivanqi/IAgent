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
        $logData = Redis::BRPOPLPUSH(self::$queueName, self::$faileQueueName, self::$maxTimeout);
        if ($logData) {
            CLog::info('logData- '. $logData);
            // 接入LogSDK,把数据发往ICollector
            try {
                if (self::$client == NULL) {
                    self::$client = new TcpClient(self::$receiverKey);
                }
                if (!self::$client->connect(self::$receiverIp, (int) self::$receiverPort)) {
                   throw new TcpClientException('连接失败');
                }
                $logData = json_decode($logData, true);
                $ret = self::$client->send($logData);
                if (!$ret) {
                    throw new TcpClientException('数据发送失败');
                }
                $msg = self::$client->recv(1024);
                if ($msg['code'] == self::$successTag) {
                    Redis::lrem(self::$faileQueueName, $logData);
                } else {
                    CLog::error($msg['msg']);
                }
            } catch(\TcpClientException $e) {
                CLog::error("无法发送数据:". $e->getMessage());
            }
        }
    }
}