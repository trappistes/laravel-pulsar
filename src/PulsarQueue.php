<?php

namespace Trappistes\Pulsar;

use DateInterval;
use DateTimeInterface;
use Exception;
use Illuminate\Container\Container;
use Illuminate\Contracts\Queue\ClearableQueue;
use Illuminate\Contracts\Queue\Queue as QueueContract;
use Illuminate\Queue\Queue;
use Illuminate\Support\Arr;
use Pulsar\Compression\Compression;
use Pulsar\Consumer;
use Pulsar\ConsumerOptions;
use Pulsar\Exception\IOException;
use Pulsar\Exception\OptionsException;
use Pulsar\Exception\RuntimeException;
use Pulsar\MessageOptions;
use Pulsar\Producer;
use Pulsar\ProducerOptions;
use Pulsar\SubscriptionType;
use Trappistes\Pulsar\Jobs\PulsarJob;

class PulsarQueue extends Queue implements ClearableQueue, QueueContract
{
    /**
     * @var array
     */
    protected array $config;

    /**
     * @var ConsumerOptions
     */
    protected ConsumerOptions $consumer_options;

    /**
     * @var ProducerOptions
     */
    protected ProducerOptions $producer_options;

    /**
     * PulsarQueue constructor.
     *
     * @param  array  $config
     *
     * @throws RuntimeException
     */
    public function __construct(array $config)
    {
        $this->config = $config;

        $this->consumer_options = $this->getConsumerOptions();

        $this->producer_options = $this->getProducerOptions();
    }

    /**
     * return the current number of jobs on the queue
     *
     * @param  null  $queue
     * @return int
     */
    public function size($queue = null): int
    {
        return 1;
    }

    /**
     * push a new job on to the queue
     *
     * @param  object|string  $job
     * @param  string  $data
     * @param  null  $queue
     * @return mixed|void
     *
     * @throws RuntimeException
     * @throws IOException
     * @throws OptionsException
     */
    public function push($job, $data = '', $queue = null)
    {
        $payload = $this->createPayload($job, $queue, $data);

        return $this->pushRaw($payload, $queue);
    }

    /**
     * push a raw payload on to the queue
     *
     * @param  string  $payload
     * @param  null  $queue
     * @param  array  $options
     * @return mixed
     *
     * @throws RuntimeException
     * @throws IOException
     * @throws OptionsException
     */
    public function pushRaw($payload, $queue = null, array $options = []): mixed
    {
        // 判断队列
        if ($queue) {
            $this->producer_options->setTopic("persistent://{$this->config['tenant']}/{$this->config['namespace']}/{$queue}");
        }

        // 实例化生产端
        $producer = new Producer("pulsar://{$this->config['endpoint']}", $this->producer_options);

        // 打开连接
        $producer->connect();

        // 判断是否延迟任务
        if ($delay = Arr::get($options, 'delay', 0)) {
            // 发送消息
            $res = $producer->send($payload, [
                MessageOptions::DELAY_SECONDS => $delay,
            ]);
        } else {
            // 发送消息
            $res = $producer->send($payload);
        }

        // 关闭连接
        $producer->close();

        return $res;
    }

    /**
     * push a new job on the queue to be processed later
     *
     * @param  DateInterval|DateTimeInterface|int  $delay
     * @param  object|string  $job
     * @param  string  $data
     * @param  null  $queue
     * @return mixed
     *
     * @throws RuntimeException
     * @throws IOException
     * @throws OptionsException
     */
    public function later($delay, $job, $data = '', $queue = null): mixed
    {
        $payload = $this->createPayload($job, $queue, $data);

        return $this->pushRaw($payload, $queue, ['delay' => $delay]);
    }

    /**
     * take a job from the queue
     *
     * @throws Exception
     */
    public function pop($queue = null): ?PulsarJob
    {
        // 判断队列
        if ($queue) {
            $this->consumer_options->setTopic("persistent://{$this->config['tenant']}/{$this->config['namespace']}/{$queue}");
        }

        // 实例化消费端
        $consumer = new Consumer("pulsar://{$this->config['endpoint']}", $this->consumer_options);

        // 建立连接
        $consumer->connect();

        // 接收消息
        $message = $consumer->receive();

        // 消费消息
        $consumer->ack($message);

        // 执行任务
        return new PulsarJob(
            $this->container ?: Container::getInstance(),
            $this,
            $message,
            $queue,
            $this->connectionName ?? null
        );
    }

    /**
     * @return ProducerOptions
     *
     * @throws RuntimeException
     */
    public function getProducerOptions(): ProducerOptions
    {
        $options = new ProducerOptions();

        // If permission authentication is available
        // Only JWT authentication is currently supported
        // $options->setAuthentication(new Jwt($config['token']));

        $options->setConnectTimeout((int) $this->config['timeout']);
        $options->setTopic("persistent://{$this->config['tenant']}/{$this->config['namespace']}/{$this->config['topic']}");
        $options->setCompression(Compression::ZLIB);

        return $options;
    }

    /**
     * 获取生产端
     *
     * @param  array  $config
     * @param  ProducerOptions  $options
     * @return Producer
     *
     * @throws OptionsException
     * @throws RuntimeException
     */
    public function getProducer(array $config, ProducerOptions $options): Producer
    {
        return new Producer("pulsar://{$config['endpoint']}", $options);
    }

    /**
     * @return ConsumerOptions
     */
    public function getConsumerOptions(): ConsumerOptions
    {
        $options = new ConsumerOptions();

        // If permission authentication is available
        // Only JWT authentication is currently supported
        // $options->setAuthentication(new Jwt($this->config['token']));

        $options->setConnectTimeout((int) $this->config['timeout']);
        $options->setTopic("persistent://{$this->config['tenant']}/{$this->config['namespace']}/{$this->config['topic']}");
        $options->setSubscription('consumer-logic');
        $options->setSubscriptionType(SubscriptionType::Shared);

        //        $options->setConsumerName('name');
        //        $options->setNackRedeliveryDelay(20);

        return $options;
    }

    /**
     * 获取消费端
     *
     * @param  array  $config
     * @param  ConsumerOptions  $options
     * @return Consumer
     *
     * @throws OptionsException
     */
    public function getConsumer(array $config, ConsumerOptions $options): Consumer
    {
        return new Consumer("pulsar://{$config['endpoint']}", $options);
    }

    /**
     * 清除队列
     *
     * @param  string  $queue
     * @return int|void
     */
    public function clear($queue)
    {
        // TODO: Implement clear() method.
    }
}
