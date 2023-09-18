<?php

namespace Trappistes\Pulsar;

interface PulsarPayload
{
    /**
     * Get the plain payload of the job.
     *
     * @return string
     */
    public function getPayload(): string;
}
