<?php declare(strict_types=1);
namespace App\Process;

use Swoft\Log\Helper\CLog;
use Swoft\Log\Helper\Log;
use Swoft\Process\Annotation\Mapping\Process;
use Swoft\Process\Contract\ProcessInterface;
use Swoole\Coroutine;
use Swoole\Process\Pool;
use App\ProcessRepositories\LogProcessRepositories;

/**
 * Class LogProcess
 *
 * @since 2.0
 *
 * @Process(workerId={0,1,2,3,4,5,6,7,8,9})
 */
class LogProcess implements ProcessInterface
{
    private static $multiConsumeSwitch = false;
    private static $multiMaxTimes = 0;

    public function __construct()
    {
        self::$multiConsumeSwitch = config('tcp.multi_consume_switch');
        self::$multiMaxTimes = config('tcp.multi_max_times');
    }
    /**
     * @param Pool $pool
     * @param int  $workerId
     */
    public function run(Pool $pool, int $workerId): void
    { 
        $logProcessRepositories = LogProcessRepositories::getInstance();
        while (true) {
            if (self::$multiConsumeSwitch) {
                for ($i = 0; $i < self::$multiMaxTimes; $i++) {
                    $logProcessRepositories->multiConsumeFunc();
                }
            } else {
                $logProcessRepositories->singleConsumeFunc();
            }
            Coroutine::sleep(0.1);
        }
    }

}