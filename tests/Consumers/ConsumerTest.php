<?php

use Ensi\LaravelPhpRdKafkaConsumer\ConsumerOptions;
use Ensi\LaravelPhpRdKafkaConsumer\Consumers\Consumer;
use Ensi\LaravelPhpRdKafkaConsumer\HighLevelConsumer;
use Ensi\LaravelPhpRdKafkaConsumer\ProcessorData;
use Ensi\LaravelPhpRdKafkaConsumer\Tests\TestConsumer;

test('consumer listen', function () {
    $highLevelConsumer = mock(HighLevelConsumer::class)
        ->shouldReceive('for', 'listen')
        ->andReturnSelf()
        ->getMock();

    $processorData = new ProcessorData(
        class: TestConsumer::class,
        topicKey: 'test',
        consumer: 'default'
    );

    $consumerOptions = new ConsumerOptions();

    $consumer = new Consumer(
        highLevelConsumer: $highLevelConsumer,
        processorData: [$processorData],
        consumerOptions: $consumerOptions,
        topicNames: ['production.domain.fact.models.1'],
        consumerName: 'default',
    );

    $consumer
        ->setMaxTime(5100)
        ->setMaxEvents(10000);

    $consumer->listen();

    expect($consumer->getConsumerOptions()->maxEvents)
        ->toBe(10000)
        ->and($consumer->getConsumerOptions()->maxTime)
        ->toBe(5100);

    $highLevelConsumer->shouldHaveReceived('for', ['default']);
    $highLevelConsumer->shouldHaveReceived('listen', [['production.domain.fact.models.1'], [$processorData], $consumerOptions]);
});
