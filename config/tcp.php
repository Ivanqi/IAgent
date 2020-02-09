<?php
return [
    'sign_key' => env('SIGN_KEY', ''),
    'receiver_key' => env('SIGN_KEY', ''),
    'receiver_ip' => env('RECEIVER_IP', '127.0.0.1'),
    'receiver_port' => env('RECEIVER_PORT', 8080),
    'multi_consume_switch' => env('MULTI_CONSUME_SWITCH', false), // 默认关闭，虽然多条消费模式，消费速度块。但是问题是无法很好处理，服务端包的最大长度的问题
    'multi_index_key' => 'project_id',
    'multi_key_records' => 'records',
    'multi_max_nums' => 5   // 暂时设置为5
];