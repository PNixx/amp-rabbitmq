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

final class MessageReceiver
{
    public const
        STATE_WAIT = 0,
        STATE_HEAD = 1,
        STATE_BODY = 2;

    /**
     * @var Buffer
     */
    private Buffer $buffer;

    /**
     * @var int
     */
    private int $state = self::STATE_WAIT;

    /**
     * @var callable[]
     */
    private array $callbacks = [];

    /**
     * @var Protocol\BasicDeliverFrame|null
     */
    private ?Protocol\BasicDeliverFrame $deliver;

    /**
     * @var Protocol\BasicReturnFrame|null
     */
    private ?Protocol\BasicReturnFrame $return;

    /**
     * @var Protocol\ContentHeaderFrame|null
     */
    private ?Protocol\ContentHeaderFrame $header;

    public function __construct(private readonly Channel $channel, private readonly Connection $connection)
    {
        $this->buffer = new Buffer;
    }

    public function start(): void
    {
        $this->onFrame(Protocol\BasicReturnFrame::class, [$this, 'receiveReturn']);
        $this->onFrame(Protocol\BasicDeliverFrame::class, [$this, 'receiveDeliver']);
        $this->onFrame(Protocol\ContentHeaderFrame::class, [$this, 'receiveHeader']);
        $this->onFrame(Protocol\ContentBodyFrame::class, [$this, 'receiveBody']);
    }

    public function stop(): void
    {
        $this->callbacks = [];
    }

    public function onMessage(callable $callback): void
    {
        $this->callbacks[] = $callback;
    }

    /**
     * @psalm-param class-string<Protocol\AbstractFrame> $frame
     */
    public function onFrame(string $frame, callable $callback): void
    {
        $this->connection->subscribe($this->channel->id(), $frame, $callback);
    }

    public function receiveReturn(Protocol\BasicReturnFrame $frame): void
    {
        if ($this->state !== self::STATE_WAIT) {
            return;
        }

        $this->return = $frame;
        $this->state = self::STATE_HEAD;
    }

    public function receiveDeliver(Protocol\BasicDeliverFrame $frame): void
    {
        if ($this->state !== self::STATE_WAIT) {
            return;
        }

        $this->deliver = $frame;
        $this->state = self::STATE_HEAD;
    }

    public function receiveHeader(Protocol\ContentHeaderFrame $frame): void
    {
        if ($this->state !== self::STATE_HEAD) {
            return;
        }

        $this->state = self::STATE_BODY;
        $this->header = $frame;

        $this->runCallbacks();
    }

    public function receiveBody(Protocol\ContentBodyFrame $frame): void
    {
        if ($this->state !== self::STATE_BODY) {
            return;
        }

        $this->buffer->append((string)$frame->payload);

        if ($this->header && $this->buffer->size() > $this->header->bodySize ) {
            $exception = Exception\ChannelException::bodyOverflow($this->buffer->size() - $this->header->bodySize, $this->buffer->flush());
            $this->channel->close();
            throw $exception;
        }

        $this->runCallbacks();
    }

    /**
     * @throws Exception\ChannelException
     */
    private function runCallbacks(): void
    {
        if (!$this->header || $this->buffer->size() !== $this->header->bodySize) {
            return;
        }

        if (isset($this->return)) {
            $message = new Message(
                $this->buffer->flush(),
                $this->return->exchange,
                $this->return->routingKey,
                null,
                null,
                false,
                true,
                $this->header->toArray()
            );
        } else {
            if ($this->deliver) {
                $message = new Message(
                    $this->buffer->flush(),
                    $this->deliver->exchange,
                    $this->deliver->routingKey,
                    $this->deliver->consumerTag,
                    $this->deliver->deliveryTag,
                    $this->deliver->redelivered,
                    false,
                    $this->header->toArray()
                );
            } else {
                throw Exception\ChannelException::frameOrder();
            }
        }

        $this->return = null;
        $this->deliver = null;
        $this->header = null;

        foreach ($this->callbacks as $callback) {
            try {
                $callback($message);
            } catch (Exception\ConsumerException $e) {
                $this->channel->nack($message);
            }
        }

        $this->state = self::STATE_WAIT;
    }
}
