<?php

use PHPinnacle\Ridge\Channel;
use PHPinnacle\Ridge\Client;
use PHPinnacle\Ridge\Config;
use PHPinnacle\Ridge\Message;
use Revolt\EventLoop;

require_once __DIR__ . '/../vendor/autoload.php';

if (!$dsn = \getenv('RIDGE_EXAMPLE_DSN')) {
    echo 'No example dsn! Please set RIDGE_EXAMPLE_DSN environment variable.', \PHP_EOL;

    exit;
}

$options = [
    'login_method' => 'EXTERNAL',
    'ssl'          => [
        'peer_name'        => '',
        'cafile'           => '',
        'local_cert'       => '',
        'local_pk'         => '',
        'verify_peer'      => true,
        'verify_peer_name' => true
    ]
];

$config = Config::parse($dsn);
$config->options = $options;

$client = new Client($config);

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
