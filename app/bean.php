<?php
/**
 * This file is part of Swoft.
 *
 * @link     https://swoft.org
 * @document https://swoft.org/docs
 * @contact  group@swoft.org
 * @license  https://github.com/swoft-cloud/swoft/blob/master/LICENSE
 */
use App\Common\DbSelector;
use App\Process\MonitorProcess;
use Swoft\Crontab\Process\CrontabProcess;
use Swoft\Db\Pool;
use Swoft\Http\Server\HttpServer;
use Swoft\Task\Swoole\SyncTaskListener;
use Swoft\Task\Swoole\TaskListener;
use Swoft\Task\Swoole\FinishListener;
use Swoft\Rpc\Client\Client as ServiceClient;
use Swoft\Rpc\Client\Pool as ServicePool;
use Swoft\Rpc\Server\ServiceServer;
use Swoft\Http\Server\Swoole\RequestListener;
use Swoft\WebSocket\Server\WebSocketServer;
use Swoft\Server\SwooleEvent;
use Swoft\Db\Database;
use Swoft\Redis\RedisDb;
use App\Listener\Test\WorkerStartListener;

return [
    'config' => [
        'path' => __DIR__ . '/../config'
    ],
    'logger'            => [
        'flushInterval' => 10,
        'flushRequest' => true,    // flushRequest 是否每个请求结束都输出日志，默认false
        'enable'       => true,    // enable 是否开启日志，默认false
        'json'         => false,    // json 是否JSON格式输出，默认false
        'handler'      => [
            'appliaction' => \bean('applicationHandler'),
            'notice'      => \bean('noticeHandler')
        ]
    ],
    'lineFormatter' => [
        'format' => '%datetime% [%level_name%] [%channel%] [%event%] [tid:%tid%] [cid:%cid%] [traceid:%traceid%] [spanid:%spanid%] [parentid:%parentid%] %messages%',
        'dateFormat' => 'Y-m-d H:i:s',
    ],
    'noticeHandler'      => [
        'class' => Swoft\Log\Handler\FileHandler::class,
        'logFile' => '@runtime/logs/notice.log',
        'formatter' => \bean('lineFormatter'),
        'levels' => 'info,debug,trace'
    ],
    'applicationHandler' => [
        'class' => Swoft\Log\Handler\FileHandler::class,
        'logFile' => '@runtime/logs/error.log',
        'formatter' => \bean('lineFormatter'),
        'levels' => 'error, warning'
    ],
    'redis'             => [
        'class'    => RedisDb::class,
        'host'     => '127.0.0.1',
        'port'     => 6379,
        'database' => 0,
        'option'   => [
            'prefix' => 'iagent:'
        ]
    ],
    'redis.pool' => [
        'class' => \Swoft\Redis\Pool::class,
        'redisDb' => \bean('redis'),
        'minActive'   => 10,
        'maxActive'   => 20,
        'maxWait'     => 0,
        'maxWaitTime' => 0,
        'maxIdleTime' => 60,
    ],
    'tcpServer'         => [
        // 'class' => TcpServer::class,
        'port'  => env('TCP_PORT', 18309),
        'debug' => env('SWOFT_DEBUG', 0),
        'on' => [
            // SwooleEvent::TASK   => bean(TaskListener::class),  
            // SwooleEvent::FINISH => bean(FinishListener::class)
        ],
        'setting' => [
            'log_file' => alias('@runtime/swoole_tcp.log'),
            // 'task_worker_num' => 1,
            // 'task_enable_coroutine' => true
        ]
    ],
    /** @see \Swoft\Tcp\Protocol */
    'tcpServerProtocol' => [
        'type' => \Swoft\Tcp\Packer\JsonPacker::TYPE,
        // 'openLengthCheck' => true,
    ],
    'processPool' => [
        'class' => \Swoft\Process\ProcessPool::class,
        'workerNum' => 5
    ]
];
