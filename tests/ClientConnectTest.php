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

use PHPinnacle\Ridge\Client;
use PHPinnacle\Ridge\Exception\ConnectionException;

class ClientConnectTest extends RidgeTest
{
    public function testConnect(): void
    {
        $client = self::client();

        $client->connect();

        self::assertTrue($client->isConnected());

        $client->disconnect();
    }

    public function testConnectWithTls(): void
    {
        $client = self::clientWithTls();

        $client->connect();

        self::assertTrue($client->isConnected());

        $client->disconnect();
    }

    public function testConnectFailure()
    {
        self::expectException(ConnectionException::class);

        $client = Client::create('amqp://127.0.0.2:5673');

        $client->connect();
    }
//
//    public function testConnectAuth()
//    {
//        $client = new Client([
//            'user' => 'testuser',
//            'password' => 'testpassword',
//            'vhost' => 'testvhost',
//        ]);
//        $client->connect();
//        $client->disconnect();
//
//        $this->assertTrue(true);
//    }
}
