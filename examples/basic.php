<?php

use PHPinnacle\Ridge\Channel;
use PHPinnacle\Ridge\Client;
use PHPinnacle\Ridge\Message;
use Revolt\EventLoop;

require __DIR__ . '/../vendor/autoload.php';

if (!$dsn = \getenv('RIDGE_EXAMPLE_DSN')) {
    echo 'No example dsn! Please set RIDGE_EXAMPLE_DSN environment variable.', \PHP_EOL;

    exit;
}

$client = Client::create($dsn);

$client->connect();

$channel = $client->channel();

$channel->queueDeclare('basic_queue', false, false, false, true);

for ($i = 0; $i < 10; $i++) {
    $channel->publish("test_$i", '', 'basic_queue');
}

$channel->consume(function (Message $message, Channel $channel) {
    echo $message->content . \PHP_EOL;

    $channel->ack($message);
}, 'basic_queue');

EventLoop::run();
