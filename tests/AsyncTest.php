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

abstract class AsyncTest extends RidgeTest
{
    protected Client $client;

    protected function setUp(): void
    {
        parent::setUp();

        $this->client = self::client();

        $this->client->connect();
    }

    protected function tearDown(): void
    {
        try {
            $this->client->disconnect();
        } catch (\Throwable $e) {}
    }
}
