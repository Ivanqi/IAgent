<?php declare(strict_types=1);
/**
 * This file is part of Swoft.
 *
 * @link     https://swoft.org
 * @document https://swoft.org/docs
 * @contact  group@swoft.org
 * @license  https://github.com/swoft-cloud/swoft/blob/master/LICENSE
 */

namespace App\Tcp\Controller;

use Swoft\Tcp\Server\Annotation\Mapping\TcpController;
use Swoft\Tcp\Server\Annotation\Mapping\TcpMapping;
// use App\Validatorg\DataValidator;
use Swoft\Tcp\Server\Request;
use Swoft\Tcp\Server\Response;
use function strrev;
use Swoft\Log\Helper\Log;
use Swoft\Redis\Redis;
use Swoft\Log\Helper\CLog;

/**
 * Class ReceiveController
 *
 * @TcpController()
 */
class ReceiveController
{
    private static $queueName;
    public function __construct()
    {
        self::$queueName = config('logjob.queue_name');
    }
    /**
     * @TcpMapping("receive", root=true)
     * @param Request  $request
     * @param Response $response
     */
    public function receive(Request $request, Response $response): void
    {
        $data = $request->getPackage()->getData();
        $result = validate($data, \DataValidator::class, [], ['DataValidator']);
        if (!$result['ret']) {
            \return_failed($response, '校验失败，非法数据');
        } else {
            Redis::lPush(self::$queueName, json_encode($data)); 
            \return_success($response);
        }
    }
}
