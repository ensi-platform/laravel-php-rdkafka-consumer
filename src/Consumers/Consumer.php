<?php

namespace Ensi\LaravelPhpRdKafkaConsumer\Consumers;

use Ensi\LaravelPhpRdKafkaConsumer\ConsumerOptions;
use Ensi\LaravelPhpRdKafkaConsumer\HighLevelConsumer;
use Ensi\LaravelPhpRdKafkaConsumer\ProcessorData;
use Throwable;

class Consumer
{
    /**
     * @param HighLevelConsumer $highLevelConsumer
     * @param ProcessorData[] $processorData
     * @param ConsumerOptions $consumerOptions
     * @param array $topicNames
     */
    public function __construct(
        protected HighLevelConsumer $highLevelConsumer,
        protected array $processorData,
        protected ConsumerOptions $consumerOptions,
        protected array $topicNames,
        protected string $consumerName,
    ) {
    }

    public function getTopicNames(): array
    {
        return $this->topicNames;
    }

    public function setMaxTime(int $maxTime = 0): self
    {
        $this->consumerOptions->maxTime = $maxTime;

        return $this;
    }

    public function setMaxEvents(int $maxEvents = 0): self
    {
        $this->consumerOptions->maxEvents = $maxEvents;

        return $this;
    }

    public function forceStop(): void
    {
        $this->highLevelConsumer->forceStop();
    }

    public function getConsumerOptions(): ConsumerOptions
    {
        return $this->consumerOptions;
    }

    /**
     * @throws Throwable
     */
    public function listen(): void
    {
        $this->highLevelConsumer
            ->for($this->consumerName)
            ->listen($this->topicNames, $this->processorData, $this->consumerOptions);
    }
}
