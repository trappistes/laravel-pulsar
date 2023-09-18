<?php

namespace Trappistes\Pulsar\Tests;

use PHPUnit\Framework\TestCase;
use Pulsar\Consumer;
use Pulsar\Message;
use Pulsar\Producer;
use Pulsar\Proto\MessageIdData;
use Trappistes\Pulsar\Connectors\PulsarConnector;
use Trappistes\Pulsar\Jobs\PulsarJob;
use Trappistes\Pulsar\PulsarQueue;

class PulsarQueueTest extends TestCase
{
    public function provider(): array
    {
        $config = [
            'driver' => 'pulsar',

            'endpoint' => env('PULSAR_ENDPOINT', '192.168.3.226:6650'),
            'timeout' => env('PULSAR_CONNECT_TIMEOUT', 5),
            'token' => env('PULSAR_TOKEN', null),

            'tenant' => env('PULSAR_TENANT', 'public'),
            'namespace' => env('PULSAR_NAMESPACE', 'default'),
            'topic' => env('PULSAR_TOPIC', 'default'),

            'subscription' => env('PULSAR_SUBSCRIPTION', 'backend'),
            'consumer_name' => env('PULSAR_CONSUMER_NAME', 'backend'),
        ];

        $connector = new PulsarConnector();

        $queue = $connector->connect($config);

        return [
            [$queue, $config],
        ];
    }

    /**
     * @test
     *
     * @dataProvider provider
     */
    public function testSize(PulsarQueue $queue, $config)
    {
        $this->assertGreaterThanOrEqual(1, $queue->size());
    }

    /**
     * @dataProvider provider
     */
    public function testPush(PulsarQueue $queue, $config)
    {
        $queue = \Mockery::mock(PulsarQueue::class)
            ->shouldAllowMockingProtectedMethods();

        $queue->expects()
            ->push('Trappistes\Pulsar\Jobs\PulsarJob@handle')
            ->andReturn(new Message(new MessageIdData(), 1, time(), 'default', 'ok'));

        $this->assertInstanceOf(
            Message::class, $queue->push('Trappistes\Pulsar\Jobs\PulsarJob@handle')
        );
    }

    /**
     * @dataProvider provider
     */
    public function testPushRaw(PulsarQueue $queue, $config)
    {
        $queue = \Mockery::mock(PulsarQueue::class)
            ->shouldAllowMockingProtectedMethods();

        $queue->expects()
            ->pushRaw('Trappistes\Pulsar\Jobs\PulsarJob@handle')
            ->andReturn(new Message(new MessageIdData(), 1, time(), 'default', 'ok'));

        $this->assertInstanceOf(
            Message::class, $queue->pushRaw('Trappistes\Pulsar\Jobs\PulsarJob@handle')
        );
    }

    /**
     * @dataProvider provider
     */
    public function testLater(PulsarQueue $queue, $config)
    {
        $queue = \Mockery::mock(PulsarQueue::class)
            ->shouldAllowMockingProtectedMethods();

        $queue->expects()
            ->later(0, 'App\Jobs\PulsarPlainJob@handle')
            ->andReturn(new Message(new MessageIdData(), 1, time(), 'default', 'ok'));

        $this->assertInstanceOf(
            Message::class, $queue->later(0, 'App\Jobs\PulsarPlainJob@handle')
        );
    }

    /**
     * @dataProvider provider
     */
    public function testPop(PulsarQueue $queue, $config)
    {
        $client = \Mockery::mock(PulsarQueue::class)
            ->shouldAllowMockingProtectedMethods();

        $client->expects()
            ->pop()
            ->andReturn(
                \Mockery::mock(PulsarJob::class)
            );

        $this->assertInstanceOf(PulsarJob::class, $client->pop());
    }

    /**
     * @dataProvider provider
     */
    public function testGetConsumer(PulsarQueue $queue, $config)
    {
        $options = $queue->getConsumerOptions();

        $this->assertInstanceOf(Consumer::class, $queue->getConsumer($config, $options));
    }

    /**
     * @dataProvider provider
     */
    public function testGetProducer(PulsarQueue $queue, $config)
    {
        $options = $queue->getProducerOptions();

        $this->assertInstanceOf(Producer::class, $queue->getProducer($config, $options));
    }
}
