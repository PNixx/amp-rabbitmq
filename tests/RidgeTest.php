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

use Amp\PHPUnit\AsyncTestCase;
use PHPinnacle\Ridge\Client;
use PHPinnacle\Ridge\Config;

abstract class RidgeTest extends AsyncTestCase
{
    /**
     * @return Client
     */
    protected static function client(): Client
    {
        if(!$dsn = \getenv('RIDGE_TEST_DSN'))
        {
            self::markTestSkipped('No test dsn! Please set RIDGE_TEST_DSN environment variable.');
        }

        $config = Config::parse($dsn);

        return new Client($config);
    }
}
