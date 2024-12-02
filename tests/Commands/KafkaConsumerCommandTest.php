<?php

use Ensi\LaravelPhpRdKafkaConsumer\Commands\KafkaConsumeCommand;
use Ensi\LaravelPhpRdKafkaConsumer\Tests\ConsumerFaker;
use Ensi\LaravelPhpRdKafkaConsumer\Tests\TestConsumer;

use function Pest\Laravel\artisan;
use function PHPUnit\Framework\assertContains;

use RdKafka\Message;

test('consume command test', function () {
    TestConsumer::fake('test-model');
    $message = new Message();
    $message->topic_name = 'production.test.fact.test-model.1';

    ConsumerFaker::new(['test-model'])
        ->addMessage($message)
        ->bind();

    artisan(KafkaConsumeCommand::class, ['topic-key' => 'test-model'])
        ->expectsOutputToContain('Start listening to topic: production.test.fact.test-model.1, consumer "default"')
        ->assertOk();

    TestConsumer::assertMessageConsumed($message);
});

test('consume command test listen multiple topics', function () {
    app()->scoped('processor-1', fn () => new TestConsumer());
    app()->scoped('processor-2', fn () => new TestConsumer());

    config()->set('kafka-consumer.processors', [
        ['topic' => 'topic-1', 'class' => 'processor-1', 'type' => 'action'],
        ['topic' => 'topic-2', 'class' => 'processor-2', 'type' => 'action'],
    ]);

    config()->set('kafka.connections.default.topics', [
        'topic-1' => "production.test.fact.topic-1.1",
        'topic-2' => "production.test.fact.topic-2.1",
    ]);

    $message1 = new Message();
    $message1->topic_name = 'production.test.fact.topic-1.1';

    $message2 = new Message();
    $message2->topic_name = 'production.test.fact.topic-2.1';

    ConsumerFaker::new(['topic-1', 'topic-2'])
        ->addMessage($message1)
        ->addMessage($message2)
        ->bind();

    artisan(KafkaConsumeCommand::class, ['topic-key' => 'topic-1,topic-2'])
        ->expectsOutputToContain('Start listening to topic: production.test.fact.topic-1.1, consumer "default"')
        ->expectsOutputToContain('Start listening to topic: production.test.fact.topic-2.1, consumer "default"')
        ->assertOk();

    assertContains($message1, resolve('processor-1')->messages);
    assertContains($message2, resolve('processor-2')->messages);
});
