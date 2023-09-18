<?php

namespace Trappistes\Pulsar\Jobs;

use Illuminate\Container\Container;
use Illuminate\Contracts\Queue\Job as JobContract;
use Illuminate\Queue\Jobs\Job;
use Pulsar\Message;
use Trappistes\Pulsar\PulsarQueue;

class PulsarJob extends Job implements JobContract
{
    /**
     * @var PulsarQueue
     */
    protected PulsarQueue $connection;

    /**
     * @var Message
     */
    protected Message $message;

    /**
     * Create a new job instance.
     *
     * @param  Container  $container
     * @param  PulsarQueue  $connection
     * @param  Message  $message
     * @param  string  $queue
     * @param  string  $connectionName
     */
    public function __construct(
        Container $container,
        PulsarQueue $connection,
        Message $message,
        string $queue,
        string $connectionName
    ) {
        $this->container = $container;
        $this->connection = $connection;
        $this->message = $message;
        $this->queue = $queue;
        $this->connectionName = $connectionName;
    }

    /**
     * 获取消息ID
     *
     * @return string
     */
    public function getJobId(): string
    {
        return $this->message->getMessageId();
    }

    /**
     * 获取消息内容
     *
     * @return string
     */
    public function getRawBody(): string
    {
        return $this->message->getPayload();
    }

    /**
     * 尝试次数
     *
     * @return int
     */
    public function attempts(): int
    {
        return 1;
    }
}
