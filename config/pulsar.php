<?php

return [
    'driver' => 'pulsar',

    'endpoint' => env('PULSAR_ENDPOINT', ''),
    'timeout' => env('PULSAR_CONNECT_TIMEOUT', 5),
    'token' => env('PULSAR_TOKEN', null),

    'tenant' => env('PULSAR_TENANT', ''),
    'namespace' => env('PULSAR_NAMESPACE', ''),
    'topic' => env('PULSAR_TOPIC', 'default'),

    'subscription' => env('PULSAR_SUBSCRIPTION', 'backend'),
    'consumer_name' => env('PULSAR_CONSUMER_NAME', 'backend'),
];
