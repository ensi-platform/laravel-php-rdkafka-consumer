<?php

namespace Ensi\LaravelPhpRdKafkaConsumer\Tests\Consumer\Topics;

use Illuminate\Support\Arr;

class Metadata
{
    protected array $topics = [];

    public function __construct(array $topicNames)
    {
        $this->topics = Arr::map($topicNames, fn ($topicName) => new Topic($topicName));
    }

    public function getTopics(): array
    {
        return $this->topics;
    }
}
