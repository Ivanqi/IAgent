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
use Swoft\Process\Annotation\Mapping\Process;
use Swoft\Process\Contract\ProcessInterface;
use Swoole\Coroutine;
use Swoole\Process\Pool;
use Swoft\Redis\Redis;

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

    public function __construct()
    {
        self::$queueName = config('logjob.queue_name');
        self::$faileQueueName = config('logjob.faile_queue_name');
        self::$maxTimeout = config('logjob.queue_max_timeout');
    }
    /**
     * @param Pool $pool
     * @param int  $workerId
     */
    public function run(Pool $pool, int $workerId): void
    { 
        while (true) {

            $this->logHandle();

            usleep(100);
        }
    }

    private function logHandle(): void
    {
        $logData = Redis::BRPOPLPUSH(self::$queueName, self::$faileQueueName, self::$maxTimeout);
        if ($logData) {
            CLog::info('logData- '. $logData);
            // 接入LogSDK,把数据发往ICollector
            Redis::lrem(self::$faileQueueName, $logData);
        }
    }
}