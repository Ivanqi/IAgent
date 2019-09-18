<?php declare(strict_types=1);
/**
 * This file is part of Swoft.
 *
 * @link     https://swoft.org
 * @document https://swoft.org/docs
 * @contact  group@swoft.org
 * @license  https://github.com/swoft-cloud/swoft/blob/master/LICENSE
 */

namespace App\Validator;

use Swoft\Validator\Annotation\Mapping\Validator;
use Swoft\Validator\Contract\ValidatorInterface;
use Swoft\Validator\Exception\ValidatorException;
use Swoft\Log\Helper\Log;
/**
 * Class CustomerValidator
 *
 * @since 2.0
 *
 * @Validator(name="DataValidator")
 */
class DataValidator implements ValidatorInterface
{
    /**
     * @param array $data
     * @param array $params
     *
     * @return array
     * @throws ValidatorException
     */
    public function validate(array $data, array $params): array
    {
        $returnFormat = ['ret' => true, 'data' => []];
        $newData = $this->checkSign($data);
        $returnFormat['ret'] = empty($newData) ? false : true;
        return $returnFormat;
    }

    private function checkSign(array $data): array
    {
        $_sign = $data['sign'];
        $_time = $data['time'];
        unset($data['sign']);
        unset($data['time']);

        $apiKey = config('tcp.sign_key');
        $str = $apiKey . '#' . $_time;
        foreach($data as $k => $v) {
            $v = is_array($v) ? json_encode($v) : $v;
            $str .= '#' . $k . '|' . $v;
        }
        if ($_sign !== md5($str)) {
            return [];
        } else {
            return $data;
        }
        return [];
    }

}
