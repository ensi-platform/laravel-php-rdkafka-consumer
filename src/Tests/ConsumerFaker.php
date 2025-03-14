<?php

namespace Ensi\LaravelPhpRdKafkaConsumer\Tests;

use Ensi\LaravelPhpRdKafka\KafkaFacade;
use Ensi\LaravelPhpRdKafka\KafkaManager as BaseKafkaManager;
use Ensi\LaravelPhpRdKafkaConsumer\Consumers\Factories\ConsumerFactory;
use Ensi\LaravelPhpRdKafkaConsumer\Exceptions\KafkaConsumerProcessorException;
use Ensi\LaravelPhpRdKafkaConsumer\HighLevelConsumer;
use Ensi\LaravelPhpRdKafkaConsumer\Tests\Consumer\KafkaConsumer;
use Ensi\LaravelPhpRdKafkaConsumer\Tests\Exceptions\OnlyTestingEnvironmentException;
use Illuminate\Support\Arr;
use RdKafka\Message;
use Throwable;

class ConsumerFaker
{
    protected array $messages = [];

    protected array $topicNames;

    public function __construct(
        protected array  $topicKeys,
        protected string $consumerName = 'default'
    ) {
        $this->topicNames = Arr::map($topicKeys, fn ($topicKey) => KafkaFacade::topicNameByClient('consumer', $consumerName, $topicKey));
    }

    public function addMessage(Message $message): self
    {
        $message->err = RD_KAFKA_RESP_ERR_NO_ERROR;

        return $this->addMessageRaw($message);
    }

    public function addMessageRaw(Message $message): self
    {
        $this->messages[] = $message;

        return $this;
    }

    /**
     * @throws KafkaConsumerProcessorException
     * @throws Throwable
     */
    public function consume(): void
    {
        $this->bind();

        (new ConsumerFactory(resolve(HighLevelConsumer::class)))
            ->build($this->topicKeys, $this->consumerName)
            ->listen();
    }

    public function bind(): void
    {
        if (!app()->runningUnitTests()) {
            throw new OnlyTestingEnvironmentException('Следует использовать только в тестировании');
        }

        app()->scoped(
            BaseKafkaManager::class,
            fn () => $this->makeKafkaManager()
        );
    }

    private function makeKafkaManager(): KafkaManager
    {
        return new KafkaManager(new KafkaConsumer($this->topicNames, $this->messages));
    }

    public static function new(array $topicKeys, string $consumerName = 'default'): self
    {
        return new self($topicKeys, $consumerName);
    }
}
