# AMQP for AMPHP V3

[![Latest Version on Packagist][ico-version]][link-packagist]
[![Software License][ico-license]](LICENSE.md)
[![Total Downloads][ico-downloads]][link-downloads]

This library is a pure asynchronous PHP implementation of the AMQP 0-9-1 protocol.

Code is mostly based on [bunnyphp](https://github.com/jakubkulhan/bunny) and [PHPinnacle Ridge](https://github.com/phpinnacle/ridge), but use [amphp](https://amphp.org) for async operations.

## Install

Via Composer

```bash
$ composer require pnixx/amp-rabbitmq
```

## Basic Usage

```php
<?php

use Amp\Loop;
use PHPinnacle\Ridge\Channel;
use PHPinnacle\Ridge\Client;
use PHPinnacle\Ridge\Message;
use Revolt\EventLoop;

require __DIR__ . '/vendor/autoload.php';

$client = Client::create('amqp://user:pass@localhost:5672');

$client->connect();

$channel = $client->channel();

$queue = $channel->queueDeclare('queue_name');

for ($i = 0; $i < 10; $i++) {
    $channel->publish("test_$i", '', 'queue_name');
}

$channel->consume(function (Message $message, Channel $channel) {
    echo $message->content() . \PHP_EOL;

    $channel->ack($message);
}, 'queue_name');

EventLoop::run();

```

More examples can be found in [`examples`](examples) directory.

## Testing

```bash
$ composer tests
```

## Change log

Please see [CHANGELOG](.github/CHANGELOG.md) for more information on what has changed recently.

## Contributing

Please see [CONTRIBUTING](.github/CONTRIBUTING.md) and [CONDUCT](.github/CONDUCT.md) for details.

## Security

If you discover any security related issues, please email dev@phpinnacle.com instead of using the issue tracker.

## Credits

- [PNixx][link-author]
- [All Contributors][link-contributors]

## License

The MIT License (MIT). Please see [License File](LICENSE.md) for more information.

[ico-version]: https://img.shields.io/packagist/v/pnixx/amp-rabbitmq.svg?style=flat-square
[ico-license]: https://img.shields.io/badge/license-MIT-brightgreen.svg?style=flat-square
[ico-downloads]: https://img.shields.io/packagist/dt/pnixx/amp-rabbitmq.svg?style=flat-square

[link-packagist]: https://packagist.org/packages/pnixx/amp-rabbitmq
[link-downloads]: https://packagist.org/packages/pnixx/amp-rabbitmq
[link-author]: https://github.com/PNixx
[link-contributors]: ../../contributors
