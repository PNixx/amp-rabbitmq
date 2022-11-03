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

use Amp\CancelledException;
use Amp\Socket\ConnectException;
use Amp\Socket\DnsSocketConnector;
use Amp\Socket\RetrySocketConnector;
use PHPinnacle\Ridge\Exception\ConnectionException;
use Revolt\EventLoop;
use Amp\Socket\ConnectContext;
use Amp\Socket\Socket;
use PHPinnacle\Ridge\Protocol\AbstractFrame;

final class Connection
{
    /**
     * @var Parser
     */
    private Parser $parser;

    /**
     * @var Socket|null
     */
    private ?Socket $socket;

    /**
     * @var callable[][][]
     * @psalm-var array<int, array<class-string<AbstractFrame>, array<string, callable>>>
     */
    private array $callbacks = [];

    /**
     * @var int
     */
    private int $lastWrite = 0;

    /**
     * @var int
     */
    private int $lastRead = 0;

    /**
     * @var \Closure|null
     */
    public ?\Closure $onClosed = null;

    /**
     * @var string|null
     */
    private ?string $heartbeatWatcherId;
    private ?string $read_loop;

    public function __construct(private readonly string $uri)
    {
        $this->parser = new Parser;
    }

    public function connected(): bool
    {
        return $this->socket !== null && $this->socket->isClosed() === false;
    }

    /**
     * @throws ConnectionException
     */
    public function write(Buffer $payload): void
    {
        $this->lastWrite = time();

        if ($this->socket !== null) {
            try {
                $this->socket->write($payload->flush());
            } catch (\Throwable $throwable) {
                $this->close();
                throw ConnectionException::writeFailed($throwable);
            }
        }
    }

    /**
     * @throws ConnectionException
     */
    public function method(int $channel, Buffer $payload): void
    {
        $this->write((new Buffer)
            ->appendUint8(1)
            ->appendUint16($channel)
            ->appendUint32($payload->size())
            ->append($payload)
            ->appendUint8(206)
        );
    }

    /**
     * @psalm-param class-string<AbstractFrame> $frame
     * @return string Callback position
     */
    public function subscribe(int $channel, string $frame, callable $callback): string
    {
        $id = uniqid();
        $this->callbacks[$channel][$frame][$id] = $callback;
        return $id;
    }

    /**
     * @param int $channel
     * @param string $frame
     * @param string $id
     */
    public function unsubscribe(int $channel, string $frame, string $id): void
    {
        unset($this->callbacks[$channel][$frame][$id]);
    }

    /**
     * @param int $channel
     */
    public function cancel(int $channel): void
    {
        unset($this->callbacks[$channel]);
    }

    /**
     * @throws ConnectionException
     */
    public function open(float $timeout, int $maxAttempts, bool $noDelay): void
    {

        $context = new ConnectContext();

        if ($timeout > 0) {
            $context = $context->withConnectTimeout($timeout);
        }

        if ($noDelay) {
            $context = $context->withTcpNoDelay();
        }

        try {
            $connector = new RetrySocketConnector(new DnsSocketConnector(), $maxAttempts);
            $this->socket = $connector->connect($this->uri, $context);
            $this->lastRead = time();

            $this->read_loop = EventLoop::onReadable($this->socket->getResource(), function (): void {
                $chunk = $this->socket->read();
                if ($chunk !== null) {
                    $this->parser->append($chunk);

                    $suspension = EventLoop::getSuspension();
                    while ($frame = $this->parser->parse()) {
                        EventLoop::defer(function () use ($frame, $suspension) {
                            try {
                                $class = \get_class($frame);
                                $this->lastRead = time();

                                /**
                                 * @psalm-var callable(AbstractFrame):bool $callback
                                 */
                                foreach ($this->callbacks[(int)$frame->channel][$class] ?? [] as $i => $callback) {
                                    if ($callback($frame)) {
                                        unset($this->callbacks[(int)$frame->channel][$class][$i]);
                                    }
                                }
                                $suspension->resume();
                            } catch (\Throwable $e) {
                                $suspension->throw($e);
                            }
                        });
                        $suspension->suspend();
                    }
                    unset($suspension);
                }
            });

        } catch (ConnectException|CancelledException $e) {
            throw ConnectionException::socketClosed($e);
        }
    }

    public function heartbeat(int $interval): void
    {
        $this->heartbeatWatcherId = EventLoop::repeat($interval, function (string $watcherId) use ($interval): void {
            $currentTime = time();

            if (null !== $this->socket) {
                $lastWrite = $this->lastWrite ?: $currentTime;

                $nextHeartbeat = $lastWrite + $interval;

                if ($currentTime >= $nextHeartbeat) {
                    $this->write((new Buffer)
                        ->appendUint8(8)
                        ->appendUint16(0)
                        ->appendUint32(0)
                        ->appendUint8(206)
                    );
                }

                unset($lastWrite, $nextHeartbeat);
            }

            if ($this->lastRead !== 0 && $currentTime > ($this->lastRead + $interval + 1)) {
                EventLoop::cancel($watcherId);
            }

            unset($currentTime);
        });
    }

    public function close(): void
    {
        $this->callbacks = [];

        if ($this->read_loop) {
            EventLoop::cancel($this->read_loop);
            $this->read_loop = null;
        }

        if (!empty($this->heartbeatWatcherId)) {
            EventLoop::cancel($this->heartbeatWatcherId);

            $this->heartbeatWatcherId = null;
        }

        $this->socket?->close();
        $this->socket = null;
    }
}
