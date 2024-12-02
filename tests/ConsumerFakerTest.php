<?php

use Ensi\LaravelPhpRdKafka\KafkaManager;
use Ensi\LaravelPhpRdKafkaConsumer\Tests\ConsumerFaker;
use Ensi\LaravelPhpRdKafkaConsumer\Tests\TestConsumer;
use RdKafka\Message;

test('create and consume kafka manager fake', function () {
    TestConsumer::fake('test-model');

    $message = new Message();
    $message->topic_name = 'production.test.fact.test-model.1';

    ConsumerFaker::new(['test-model'])
        ->addMessage($message)
        ->consume();

    TestConsumer::assertMessageConsumed($message);
});

test('bind testing kafka manager with faker consumer', function () {
    TestConsumer::fake('test-model');

    $message = new Message();
    $message->topic_name = 'production.test.fact.test-model.1';

    ConsumerFaker::new(['test-model'])
        ->addMessage($message)
        ->bind();

    expect(resolve(KafkaManager::class))
        ->toBeInstanceOf(\Ensi\LaravelPhpRdKafkaConsumer\Tests\KafkaManager::class);
});
