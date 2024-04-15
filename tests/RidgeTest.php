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
        if (!$dsn = \getenv('RIDGE_TEST_DSN')) {
            self::markTestSkipped('No test dsn! Please set RIDGE_TEST_DSN environment variable.');
        }

        $config = Config::parse($dsn);

        return new Client($config);
    }

    protected static function clientWithTls(): Client
    {
        if (!$dsn = \getenv('RIDGE_TEST_DSN')) {
            self::markTestSkipped('No test dsn! Please set RIDGE_TEST_DSN environment variable.');
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

        return new Client($config);
    }
}
