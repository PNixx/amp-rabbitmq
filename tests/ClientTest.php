<?php
/**
 * This file is part of PHPinnacle/Ridge.
 *
 * (c) PHPinnacle Team <dev@phpinnacle.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace PHPinnacle\Ridge\Tests;

use PHPinnacle\Ridge\Channel;
use PHPinnacle\Ridge\Client;
use PHPinnacle\Ridge\Exception\ChannelException;
use PHPinnacle\Ridge\Message;
use Revolt\EventLoop;

class ClientTest extends AsyncTest
{
    public function testOpenChannel(): void
    {
        self::assertInstanceOf(Channel::class, $this->client->channel());
    }

    public function testOpenMultipleChannel(): void
    {
        $channel1 = $this->client->channel();
        $channel2 = $this->client->channel();

        self::assertInstanceOf(Channel::class, $channel1);
        self::assertInstanceOf(Channel::class, $channel2);
        self::assertNotEquals($channel1->id(), $channel2->id());

        $channel3 = $this->client->channel();

        self::assertInstanceOf(Channel::class, $channel3);
        self::assertNotEquals($channel1->id(), $channel3->id());
        self::assertNotEquals($channel2->id(), $channel3->id());
    }
}
