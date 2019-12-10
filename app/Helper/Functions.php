<?php
/**
 * This file is part of Swoft.
 *
 * @link     https://swoft.org
 * @document https://swoft.org/docs
 * @contact  group@swoft.org
 * @license  https://github.com/swoft-cloud/swoft/blob/master/LICENSE
 */
const TCP_ERROR = 1;
const TCP_SUCCESS = 0;

function user_func(): string
{
    return 'hello';
}

/**
 * @param Swoft\Tcp\Server\Response $response
 * @param string $msg 返回信息
 * @param mixed $data 返回的数据
 */
function return_failed($response, string $msg = '', $data = ''): void
{
    $errorMsg = empty($msg) ? 'Failed' : $msg;
    $response->setCode(TCP_ERROR);
    $response->setMsg($errorMsg);
    $response->setData($data);
}

/**
 * @param Swoft\Tcp\Server\Response $response
 * @param string $msg 返回信息
 * @param mixed $data 返回的数据
 */
function return_success($response, string $msg = '', $data = ''): void
{
    $errorMsg = empty($msg) ? 'Success' : $msg;
    $response->setCode(TCP_SUCCESS);
    $response->setMsg($errorMsg);
    $response->setData($data);
}
