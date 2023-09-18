<?php

namespace Trappistes\Pulsar\Connectors;

use Illuminate\Queue\Connectors\ConnectorInterface;
use Pulsar\Exception\OptionsException;
use Pulsar\Exception\RuntimeException;
use Trappistes\Pulsar\PulsarQueue;

class PulsarConnector implements ConnectorInterface
{
    /**
     * @param  array  $config
     * @return PulsarQueue
     */
    public function connect(array $config): PulsarQueue
    {
        try {
            return new PulsarQueue($config);
        } catch (OptionsException|RuntimeException $e) {
        }
    }
}
