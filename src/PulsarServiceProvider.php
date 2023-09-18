<?php

namespace Trappistes\Pulsar;

use Illuminate\Support\ServiceProvider;
use Trappistes\Pulsar\Connectors\PulsarConnector;

class PulsarServiceProvider extends ServiceProvider
{
    /**
     * Register services.
     *
     * @return void
     */
    public function register()
    {
        $this->mergeConfigFrom(
            __DIR__.'/../config/pulsar.php', 'queue.connections.pulsar'
        );
    }

    /**
     * Bootstrap services.
     *
     * @return void
     */
    public function boot()
    {
        $queue_manager = $this->app['queue'];

        $queue_manager->addConnector('pulsar', function () {
            return new PulsarConnector();
        });
    }
}
