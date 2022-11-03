<?php
/**
 * This file is part of PHPinnacle/Ridge.
 *
 * (c) PHPinnacle Team <dev@phpinnacle.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

declare(strict_types=1);

namespace PHPinnacle\Ridge;

use Revolt\EventLoop;
use Revolt\EventLoop\Suspension;

final class Client
{
    public static string $product = 'Ridge';

    private const STATE_NOT_CONNECTED = 0;
    private const STATE_CONNECTING = 1;
    private const STATE_CONNECTED = 2;
    private const STATE_DISCONNECTING = 3;

    private const CONNECTION_MONITOR_INTERVAL = 5;

    /**
     * @var int
     */
    private int $state = self::STATE_NOT_CONNECTED;

    /**
     * @var Channel[]
     */
    private array $channels = [];

    /**
     * @var int
     */
    private int $nextChannelId = 1;

    /**
     * @var Connection
     */
    private Connection $connection;

    /**
     * @var Properties
     */
    private Properties $properties;

    /**
     * @var string|null
     */
    private ?string $connectionMonitorWatcherId;

    public function __construct(private readonly Config $config)
    {
    }

    public static function create(string $dsn): self
    {
        return new self(Config::parse($dsn));
    }

    /**
     * @throws Exception\ClientException
     */
    public function properties(): Properties
    {
        if ($this->state !== self::STATE_CONNECTED) {
            throw Exception\ClientException::notConnected();
        }

        return $this->properties;
    }

    /**
     * @return void
     *
     * @throws Exception\ClientException
     */
    public function connect(): void
    {
        if ($this->state !== self::STATE_NOT_CONNECTED) {
            throw Exception\ClientException::alreadyConnected();
        }

        $this->state = self::STATE_CONNECTING;

        $this->channels = [];
        $this->connection = new Connection($this->config->uri());

        $this->connection->open(
            $this->config->timeout,
            $this->config->tcpAttempts,
            $this->config->tcpNoDelay
        );

        $buffer = new Buffer;
        $buffer
            ->append('AMQP')
            ->appendUint8(0)
            ->appendUint8(0)
            ->appendUint8(9)
            ->appendUint8(1);

        $this->connection->write($buffer);

        $this->connectionStart();
        $this->connectionTune();
        $this->connectionOpen();

        //Subscribe to close frame
        $this->connection->subscribe(0, Protocol\ConnectionCloseFrame::class, function (Protocol\ConnectionCloseFrame $frame) {
            try {
                $buffer = new Buffer;
                $buffer
                    ->appendUint8(1)
                    ->appendUint16(0)
                    ->appendUint32(4)
                    ->appendUint16(10)
                    ->appendUint16(51)
                    ->appendUint8(206);

                $this->connection->write($buffer);
            } catch (Exception\ConnectionException $e) {
            }
            $this->connection->close();

            $this->disableConnectionMonitor();
            $this->channels = [];
            $this->state = self::STATE_NOT_CONNECTED;

            if( $this->connection->onClosed ) {
                call_user_func($this->connection->onClosed, $frame->replyText);
            } else {
                throw Exception\ClientException::disconnected($frame->replyText);
            }
        });

        $this->state = self::STATE_CONNECTED;

        $this->connectionMonitorWatcherId = EventLoop::repeat(
            self::CONNECTION_MONITOR_INTERVAL,
            function (): void {
                if ($this->connection->connected() === false) {
                    $this->state = self::STATE_NOT_CONNECTED;
                    $this->channels = [];

                    throw Exception\ClientException::disconnected('Monitor watcher disconnected');
                }
            }
        );
    }

    /**
     * @throws Exception\ClientException
     */
    public function disconnect(int $code = 0, string $reason = ''): void
    {
        $this->disableConnectionMonitor();

        try {
            if (\in_array($this->state, [self::STATE_NOT_CONNECTED, self::STATE_DISCONNECTING])) {
                return;
            }

            if ($this->state !== self::STATE_CONNECTED) {
                throw Exception\ClientException::notConnected();
            }

            if ($this->connectionMonitorWatcherId !== null) {
                EventLoop::cancel($this->connectionMonitorWatcherId);

                $this->connectionMonitorWatcherId = null;
            }

            $this->state = self::STATE_DISCONNECTING;

            if ($code === 0) {
                foreach ($this->channels as $channel) {
                    $channel->close($code, $reason);
                }
            }

            $this->connectionClose($code, $reason);

            $this->connection->close();
        } finally {
            $this->state = self::STATE_NOT_CONNECTED;
        }
    }

    /**
     * @return Channel
     *
     * @throws Exception\ClientException
     */
    public function channel(): Channel
    {
        if ($this->state !== self::STATE_CONNECTED) {
            throw Exception\ClientException::notConnected();
        }

        try {
            $id = $this->findChannelId();
            $channel = new Channel($id, $this->connection, $this->properties);

            $this->channels[$id] = $channel;

            $channel->open();
            $channel->qos($this->config->qosSize, $this->config->qosCount, $this->config->qosGlobal);

            $close_callback = function () use ($id) {
                $this->connection->cancel($id);
                unset($this->channels[$id]);
            };
            $this->connection->subscribe($id, Protocol\ChannelCloseFrame::class, $close_callback);
            $this->connection->subscribe($id, Protocol\ChannelCloseOkFrame::class, $close_callback);

            return $channel;
        } catch (Exception\ConnectionException $exception) {
            $this->state = self::STATE_NOT_CONNECTED;

            throw $exception;
        } catch (\Throwable $error) {
            throw Exception\ClientException::unexpectedResponse($error);
        }
    }

    public function isConnected(): bool
    {
        return $this->state === self::STATE_CONNECTED && $this->connection->connected();
    }

    /**
     * @throws Exception\ClientException
     */
    private function connectionStart(): void
    {

        $start = $this->await(Protocol\ConnectionStartFrame::class);

        if (!\str_contains($start->mechanisms, 'AMQPLAIN')) {
            throw Exception\ClientException::notSupported($start->mechanisms);
        }

        $this->properties = Properties::create($start->serverProperties);

        $buffer = new Buffer;
        $buffer
            ->appendTable([
                'LOGIN' => $this->config->user,
                'PASSWORD' => $this->config->pass,
            ])
            ->discard(4);

        $frameBuffer = new Buffer;
        $frameBuffer
            ->appendUint16(10)
            ->appendUint16(11)
            ->appendTable(['product' => Client::$product, 'platform' => 'PHP ' . (phpversion() ?: 'N/A')])
            ->appendString('AMQPLAIN')
            ->appendText((string)$buffer)
            ->appendString('en_US');

        $this->connection->method(0, $frameBuffer);
    }

    /**
     * @return void
     */
    private function connectionTune(): void
    {
        /** @var Protocol\ConnectionTuneFrame $tune */
        $tune = $this->await(Protocol\ConnectionTuneFrame::class);

        $heartbeatInterval = $this->config->heartbeat;

        if ($heartbeatInterval !== 0) {
            $heartbeatInterval = \min($heartbeatInterval, $tune->heartbeat);
        }

        $maxChannel = \min($this->config->maxChannel, $tune->channelMax);
        $maxFrame = \min($this->config->maxFrame, $tune->frameMax);

        $buffer = new Buffer;
        $buffer
            ->appendUint8(1)
            ->appendUint16(0)
            ->appendUint32(12)
            ->appendUint16(10)
            ->appendUint16(31)
            ->appendInt16($maxChannel)
            ->appendInt32($maxFrame)
            ->appendInt16($heartbeatInterval)
            ->appendUint8(206);

        $this->connection->write($buffer);

        $this->properties->tune($maxChannel, $maxFrame);

        if ($heartbeatInterval > 0) {
            $this->connection->heartbeat($heartbeatInterval);
        }
    }

    /**
     * @return void
     */
    private function connectionOpen(): void
    {
        $vhost = $this->config->vhost;
        $capabilities = '';
        $insist = false;

        $buffer = new Buffer;
        $buffer
            ->appendUint8(1)
            ->appendUint16(0)
            ->appendUint32(7 + \strlen($vhost) + \strlen($capabilities))
            ->appendUint16(10)
            ->appendUint16(40)
            ->appendString($vhost)
            ->appendString($capabilities) // TODO: process server capabilities
            ->appendBits([$insist])
            ->appendUint8(206);

        $this->connection->write($buffer);

        $this->await(Protocol\ConnectionOpenOkFrame::class);
    }

    /**
     * @param int $code
     * @param string $reason
     */
    private function connectionClose(int $code, string $reason): void
    {
        $buffer = new Buffer;
        $buffer
            ->appendUint8(1)
            ->appendUint16(0)
            ->appendUint32(11 + \strlen($reason))
            ->appendUint16(10)
            ->appendUint16(50)
            ->appendInt16($code)
            ->appendString($reason)
            ->appendInt16(0)
            ->appendInt16(0)
            ->appendUint8(206);

        $this->connection->write($buffer);

        $this->await(Protocol\ConnectionCloseOkFrame::class);
    }

    /**
     * @return int
     */
    private function findChannelId(): int
    {
        /** first check in range [next, max] ... */
        for ($id = $this->nextChannelId; $id <= $this->config->maxChannel; ++$id) {
            if (!isset($this->channels[$id])) {
                $this->nextChannelId = $id + 1;

                return $id;
            }
        }

        /** then check in range [min, next) ... */
        for ($id = 1; $id < $this->nextChannelId; ++$id) {
            if (!isset($this->channels[$id])) {
                $this->nextChannelId = $id + 1;

                return $id;
            }
        }

        throw Exception\ClientException::noChannelsAvailable();
    }

    /**
     * @template T of Protocol\AbstractFrame
     * @psalm-param class-string<T> $frame
     * @psalm-return T
     * @param string $frame
     * @return Protocol\AbstractFrame
     */
    private function await(string $frame): Protocol\AbstractFrame
    {
        /** @psalm-var Suspension<T> $suspension */
        $suspension = EventLoop::getSuspension();
        $timeout = EventLoop::delay($this->config->timeout, static fn() => $suspension->throw(new Exception\ClientException('Frame wait timeout')));
        $this->connection->subscribe(0, $frame, static function (Protocol\AbstractFrame $frame) use ($timeout, $suspension) {
            EventLoop::cancel($timeout);
            /** @psalm-var T $frame */
            $suspension->resume($frame);

            return true;
        });

        return $suspension->suspend();
    }

    private function disableConnectionMonitor(): void
    {
        if ($this->connectionMonitorWatcherId !== null) {

            EventLoop::cancel($this->connectionMonitorWatcherId);

            $this->connectionMonitorWatcherId = null;
        }
    }
}
