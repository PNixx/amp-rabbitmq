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

use Amp\Cancellation;
use Amp\CancelledException;
use Amp\CompositeCancellation;
use Amp\DeferredCancellation;
use Amp\DeferredFuture;
use Amp\Future;
use Amp\TimeoutCancellation;
use PHPinnacle\Ridge\Exception\ChannelException;
use PHPinnacle\Ridge\Exception\ClientException;
use PHPinnacle\Ridge\Exception\ProtocolException;
use PHPinnacle\Ridge\Protocol\AbstractFrame;

class Channel
{
    private const STATE_READY = 1;
    private const STATE_OPEN = 2;
    private const STATE_CLOSING = 3;
    private const STATE_CLOSED = 4;
    private const STATE_ERROR = 5;

    /** Regular AMQP guarantees of published messages delivery.  */
    private const MODE_REGULAR = 1;
    /** Messages are published after 'tx.commit'. */
    private const MODE_TRANSACTIONAL = 2;
    /** Broker sends asynchronously 'basic.ack's for delivered messages. */
    private const MODE_CONFIRM = 3;

    private const AWAIT_TIMEOUT = 300;

    /**
     * @var int
     */
    private int $id;

    /**
     * @var Connection
     */
    private Connection $connection;

    /**
     * @var Properties
     */
    private Properties $properties;

    /**
     * @var int
     */
    private int $state = self::STATE_READY;

    /**
     * @var int
     */
    private int $mode = self::MODE_REGULAR;

    /**
     * @var MessageReceiver
     */
    private MessageReceiver $receiver;

    /**
     * @var Consumer
     */
    private Consumer $consumer;

    /**
     * @var Events
     */
    private Events $events;

    /**
     * @var DeferredCancellation
     */
    private DeferredCancellation $cancellation;

    /**
     * @var int
     */
    private int $deliveryTag = 0;

		private static bool $getting = false;

    public function __construct(int $id, Connection $connection, Properties $properties)
    {
        $this->id = $id;
        $this->connection = $connection;
        $this->properties = $properties;
        $this->receiver = new MessageReceiver($this, $connection);
        $this->consumer = new Consumer($this, $this->receiver);
        $this->events = new Events($this, $this->receiver);
        $this->cancellation = new DeferredCancellation;
    }

    public function __destruct()
    {
        $this->cancellation->cancel(new ChannelException('Channel #' . $this->id . ' was closed on destruct'));
    }

    public function isClosed(): bool
    {
        return in_array($this->state, [self::STATE_CLOSING, self::STATE_CLOSED, self::STATE_ERROR]);
    }

    public function id(): int
    {
        return $this->id;
    }

    public function events(): Events
    {
        return $this->events;
    }

    public function cancellation(): Cancellation
    {
        return $this->cancellation->getCancellation();
    }

    /**
     * @throws Exception\ChannelException
     */
    public function open(string $outOfBand = ''): void
    {
				self::$getting = false;
        if ($this->state !== self::STATE_READY) {
            throw Exception\ChannelException::notReady($this->id);
        }

        $this->write((new Buffer)
            ->appendUint8(1)
            ->appendUint16($this->id)
            ->appendUint32(5 + \strlen($outOfBand))
            ->appendUint16(20)
            ->appendUint16(10)
            ->appendString($outOfBand)
            ->appendUint8(206),
            Protocol\ChannelOpenOkFrame::class
        );

        $this->receiver->start();
        $this->consumer->start();

        $this->connection->subscribe($this->id, Protocol\ChannelCloseFrame::class, function (Protocol\ChannelCloseFrame $frame) {
            $this->state = self::STATE_CLOSED;
            $this->connection->cancel($this->id);
            $this->cancellation->cancel(new ChannelException('Channel #' . $this->id . ' was closed: ' . $frame->replyText));

            $buffer = new Buffer;
            $buffer
                ->appendUint8(1)
                ->appendUint16($this->id)
                ->appendUint32(4)
                ->appendUint16(20)
                ->appendUint16(41)
                ->appendUint8(206);

            $this->connection->write($buffer);
        });

        $this->state = self::STATE_OPEN;
    }

    /**
     * @throws Exception\ChannelException
     */
    public function close(int $code = 0, string $reason = ''): void
    {
				self::$getting = false;
        if ($this->state === self::STATE_CLOSED) {
            throw Exception\ChannelException::alreadyClosed($this->id);
        }

        if ($this->state === self::STATE_CLOSING) {
            return;
        }

        $this->state = self::STATE_CLOSING;

        $this->receiver->stop();
        $this->consumer->stop();
        $this->cancellation->cancel(new ChannelException('Closing channel #' . $this->id));

        $this->write((new Buffer)
            ->appendUint8(1)
            ->appendUint16($this->id)
            ->appendUint32(11 + \strlen($reason))
            ->appendUint16(20)
            ->appendUint16(40)
            ->appendInt16($code)
            ->appendString($reason)
            ->appendInt16(0)
            ->appendInt16(0)
            ->appendUint8(206),
            Protocol\ChannelCloseOkFrame::class
        );

        $this->connection->cancel($this->id);

        $this->state = self::STATE_CLOSED;
    }

    /**
     * @param int $prefetchSize
     * @param int $prefetchCount
     * @param bool $global
     */
    public function qos(int $prefetchSize = 0, int $prefetchCount = 0, bool $global = false): void
    {
        $this->write((new Buffer)
            ->appendUint8(1)
            ->appendUint16($this->id)
            ->appendUint32(11)
            ->appendUint16(60)
            ->appendUint16(10)
            ->appendInt32($prefetchSize)
            ->appendInt16($prefetchCount)
            ->appendBits([$global])
            ->appendUint8(206),
            Protocol\BasicQosOkFrame::class
        );
    }

    /**
     * @param callable $callback
     * @param string $queue
     * @param string $consumerTag
     * @param bool $noLocal
     * @param bool $noAck
     * @param bool $exclusive
     * @param bool $noWait
     * @param array $arguments
     * @return string
     * @throws ChannelException
     */
    public function consume
    (
        callable $callback,
        string   $queue = '',
        string   $consumerTag = '',
        bool     $noLocal = false,
        bool     $noAck = false,
        bool     $exclusive = false,
        bool     $noWait = false,
        array    $arguments = []
    ): string
    {
        $flags = [$noLocal, $noAck, $exclusive, $noWait];

        //Subscribe before send request
        if( $consumerTag ) {
            $this->consumer->subscribe($consumerTag, $callback);
        }

        $payload = (new Buffer)
            ->appendUint16(60)
            ->appendUint16(20)
            ->appendInt16(0)
            ->appendString($queue)
            ->appendString($consumerTag)
            ->appendBits($flags)
            ->appendTable($arguments);

        if ($noWait) {
            if( empty($consumerTag) ) {
                throw new ProtocolException('Consumer tag can\'t be null with no wait flag');
            }
            $this->method($payload);
        } else {
            $frame = $this->method($payload, Protocol\BasicConsumeOkFrame::class, Protocol\ChannelCloseFrame::class);

            if ($frame instanceof Protocol\ChannelCloseFrame) {
                throw new ChannelException($frame->replyText, $frame->replyCode);
            }

            if ($consumerTag === '') {
                /** @var Protocol\BasicConsumeOkFrame $frame */
                $this->consumer->subscribe($frame->consumerTag, $callback);
            }
        }

        return $consumerTag;
    }

    /**
     * @param string $consumerTag
     * @param bool $noWait
     */
    public function cancel(string $consumerTag, bool $noWait = false): void
    {
        $buffer = (new Buffer)
            ->appendUint8(1)
            ->appendUint16($this->id)
            ->appendUint32(6 + \strlen($consumerTag))
            ->appendUint16(60)
            ->appendUint16(30)
            ->appendString($consumerTag)
            ->appendBits([$noWait])
            ->appendUint8(206);

        if ($noWait) {
            $this->write($buffer);
        } else {
            $this->write($buffer, Protocol\BasicCancelOkFrame::class);
        }

        $this->consumer->cancel($consumerTag);
    }

    /**
     * @param Message $message
     * @param bool $multiple
     * @throws \PHPinnacle\Ridge\Exception\ProtocolException
     */
    public function ack(Message $message, bool $multiple = false): void
    {
        if ($message->deliveryTag === null) {
            throw ProtocolException::unsupportedDeliveryTag();
        }

        $this->write((new Buffer)
            ->appendUint8(1)
            ->appendUint16($this->id)
            ->appendUint32(13)
            ->appendUint16(60)
            ->appendUint16(80)
            ->appendInt64($message->deliveryTag)
            ->appendBits([$multiple])
            ->appendUint8(206)
        );
    }

    /**
     * @param Message $message
     * @param bool $multiple
     * @param bool $requeue
     * @throws \PHPinnacle\Ridge\Exception\ProtocolException
     */
    public function nack(Message $message, bool $multiple = false, bool $requeue = true): void
    {
        if ($message->deliveryTag === null) {
            throw ProtocolException::unsupportedDeliveryTag();
        }

        $this->write((new Buffer)
            ->appendUint8(1)
            ->appendUint16($this->id)
            ->appendUint32(13)
            ->appendUint16(60)
            ->appendUint16(120)
            ->appendInt64($message->deliveryTag)
            ->appendBits([$multiple, $requeue])
            ->appendUint8(206)
        );
    }

    /**
     * @param Message $message
     * @param bool $requeue
     * @throws \PHPinnacle\Ridge\Exception\ProtocolException
     */
    public function reject(Message $message, bool $requeue = true): void
    {
        if ($message->deliveryTag === null) {
            throw ProtocolException::unsupportedDeliveryTag();
        }

        $this->write((new Buffer)
            ->appendUint8(1)
            ->appendUint16($this->id)
            ->appendUint32(13)
            ->appendUint16(60)
            ->appendUint16(90)
            ->appendInt64($message->deliveryTag)
            ->appendBits([$requeue])
            ->appendUint8(206)
        );
    }

    /**
     * @param bool $requeue
     */
    public function recover(bool $requeue = false): void
    {
        $this->write((new Buffer)
            ->appendUint8(1)
            ->appendUint16($this->id)
            ->appendUint32(5)
            ->appendUint16(60)
            ->appendUint16(110)
            ->appendBits([$requeue])
            ->appendUint8(206),
            Protocol\BasicRecoverOkFrame::class
        );
    }

    /**
     * @param string $queue
     * @param bool $noAck
     * @return Message|null
     */
    public function get(string $queue = '', bool $noAck = false): ?Message
    {
        $this->throwIfClosed();

        if (self::$getting) {
            throw Exception\ChannelException::getInProgress();
        }

        self::$getting = true;

        /** @var Protocol\BasicGetOkFrame|Protocol\BasicGetEmptyFrame $frame */
        $frame = $this->write((new Buffer)
            ->appendUint8(1)
            ->appendUint16($this->id)
            ->appendUint32(8 + \strlen($queue))
            ->appendUint16(60)
            ->appendUint16(70)
            ->appendInt16(0)
            ->appendString($queue)
            ->appendBits([$noAck])
            ->appendUint8(206),
            Protocol\BasicGetOkFrame::class,
            Protocol\BasicGetEmptyFrame::class,
            Protocol\ChannelCloseFrame::class,
        );

        if ($frame instanceof Protocol\ChannelCloseFrame) {
            self::$getting = false;
            throw new ChannelException($frame->replyText, $frame->replyCode);
        }

        if ($frame instanceof Protocol\BasicGetEmptyFrame) {
            self::$getting = false;
            return null;
        }

        /** @var Protocol\ContentHeaderFrame $header */
        $header = $this->await(Protocol\ContentHeaderFrame::class)->await(new CompositeCancellation($this->cancellation(), new TimeoutCancellation(self::AWAIT_TIMEOUT)));

        $buffer = new Buffer;
        $remaining = $header->bodySize;

        while ($remaining > 0) {
            /** @var Protocol\ContentBodyFrame $body */
            $body = $this->await(Protocol\ContentBodyFrame::class)->await(new CompositeCancellation($this->cancellation(), new TimeoutCancellation(self::AWAIT_TIMEOUT)));

            $buffer->append((string)$body->payload);

            $remaining -= (int)$body->size;

            if ($remaining < 0) {
                $this->state = self::STATE_ERROR;
                $this->close();
                throw Exception\ChannelException::bodyOverflow($remaining, $buffer->flush());
            }
        }

        self::$getting = false;
        return new Message(
            $buffer->flush(),
            $frame->exchange,
            $frame->routingKey,
            null,
            $frame->deliveryTag,
            $frame->redelivered,
            false,
            $header->toArray()
        );
    }

    /**
     * @param string $body
     * @param string $exchange
     * @param string $routingKey
     * @param array $headers
     * @param bool $mandatory
     * @param bool $immediate
     * @return int|null
     */
    public function publish
    (
        string $body,
        string $exchange = '',
        string $routingKey = '',
        array  $headers = [],
        bool   $mandatory = false,
        bool   $immediate = false
    ): ?int
    {
        $this->doPublish($body, $exchange, $routingKey, $headers, $mandatory, $immediate);

        return $this->mode === self::MODE_CONFIRM ? ++$this->deliveryTag : null;
    }

    /**
     * @throws Exception\ChannelException
     */
    public function txSelect(): void
    {
        if ($this->mode !== self::MODE_REGULAR) {
            throw Exception\ChannelException::notRegularFor("transactional");
        }

        $this->write((new Buffer)
            ->appendUint8(1)
            ->appendUint16($this->id)
            ->appendUint32(4)
            ->appendUint16(90)
            ->appendUint16(10)
            ->appendUint8(206),
            Protocol\TxSelectOkFrame::class
        );

        $this->mode = self::MODE_TRANSACTIONAL;
    }

    /**
     * @throws Exception\ChannelException
     */
    public function txCommit(): void
    {
        if ($this->mode !== self::MODE_TRANSACTIONAL) {
            throw Exception\ChannelException::notTransactional();
        }

        $this->write((new Buffer)
            ->appendUint8(1)
            ->appendUint16($this->id)
            ->appendUint32(4)
            ->appendUint16(90)
            ->appendUint16(20)
            ->appendUint8(206),
            Protocol\TxCommitOkFrame::class
        );
    }

    /**
     * @throws Exception\ChannelException
     */
    public function txRollback(): void
    {
        if ($this->mode !== self::MODE_TRANSACTIONAL) {
            throw Exception\ChannelException::notTransactional();
        }

        $this->write((new Buffer)
            ->appendUint8(1)
            ->appendUint16($this->id)
            ->appendUint32(4)
            ->appendUint16(90)
            ->appendUint16(30)
            ->appendUint8(206),
            Protocol\TxRollbackOkFrame::class
        );
    }

    /**
     * @throws Exception\ChannelException
     */
    public function confirmSelect(bool $noWait = false): void
    {
        if ($this->mode !== self::MODE_REGULAR) {
            throw Exception\ChannelException::notRegularFor("confirm");
        }

        $buffer = (new Buffer)
            ->appendUint8(1)
            ->appendUint16($this->id)
            ->appendUint32(5)
            ->appendUint16(85)
            ->appendUint16(10)
            ->appendBits([$noWait])
            ->appendUint8(206);

        if ($noWait) {
            $this->write($buffer);
        } else {
            $this->write($buffer, Protocol\ConfirmSelectOkFrame::class);
        }

        $this->mode = self::MODE_CONFIRM;
        $this->deliveryTag = 0;
    }

    /**
     * @param string $queue
     * @param bool $passive
     * @param bool $durable
     * @param bool $exclusive
     * @param bool $autoDelete
     * @param bool $noWait
     * @param array $arguments
     * @return Queue|null
     */
    public function queueDeclare
    (
        string $queue = '',
        bool   $passive = false,
        bool   $durable = false,
        bool   $exclusive = false,
        bool   $autoDelete = false,
        bool   $noWait = false,
        array  $arguments = []
    ): ?Queue
    {
        $flags = [$passive, $durable, $exclusive, $autoDelete, $noWait];

        $buffer = (new Buffer)
            ->appendUint16(50)
            ->appendUint16(10)
            ->appendInt16(0)
            ->appendString($queue)
            ->appendBits($flags)
            ->appendTable($arguments);

        if ($noWait) {
            $this->method($buffer);
            return null;
        }

        $frame = $this->method($buffer, Protocol\QueueDeclareOkFrame::class, Protocol\ChannelCloseFrame::class);

        if ($frame instanceof Protocol\ChannelCloseFrame) {
            throw new ChannelException($frame->replyText, $frame->replyCode);
        }

        /** @var Protocol\QueueDeclareOkFrame $frame */
        return new Queue($frame->queue, $frame->messageCount, $frame->consumerCount);
    }

    /**
     * @param string $queue
     * @param string $exchange
     * @param string $routingKey
     * @param bool $noWait
     * @param array $arguments
     */
    public function queueBind
    (
        string $queue = '',
        string $exchange = '',
        string $routingKey = '',
        bool   $noWait = false,
        array  $arguments = []
    ): void
    {
        $buffer = (new Buffer)
            ->appendUint16(50)
            ->appendUint16(20)
            ->appendInt16(0)
            ->appendString($queue)
            ->appendString($exchange)
            ->appendString($routingKey)
            ->appendBits([$noWait])
            ->appendTable($arguments);

        if( $noWait ) {
            $this->method($buffer);
        } else {
            $this->method($buffer, Protocol\QueueBindOkFrame::class);
        }
    }

    /**
     * @param string $queue
     * @param string $exchange
     * @param string $routingKey
     * @param bool $noWait
     * @param array $arguments
     */
    public function queueUnbind
    (
        string $queue = '',
        string $exchange = '',
        string $routingKey = '',
        bool   $noWait = false,
        array  $arguments = []
    ): void
    {
        $buffer = (new Buffer)
            ->appendUint16(50)
            ->appendUint16(50)
            ->appendInt16(0)
            ->appendString($queue)
            ->appendString($exchange)
            ->appendString($routingKey)
            ->appendTable($arguments);

        if( $noWait ) {
            $this->method($buffer);
        } else {
            $this->method($buffer, Protocol\QueueUnbindOkFrame::class);
        }
    }

    /**
     * @param string $queue
     * @param bool $noWait
     * @return int
     */
    public function queuePurge(string $queue = '', bool $noWait = false): int
    {
        $buffer = (new Buffer)
            ->appendUint8(1)
            ->appendUint16($this->id)
            ->appendUint32(8 + \strlen($queue))
            ->appendUint16(50)
            ->appendUint16(30)
            ->appendInt16(0)
            ->appendString($queue)
            ->appendBits([$noWait])
            ->appendUint8(206);

        if ($noWait) {
            $this->write($buffer);
            return 0;
        }

        /** @var Protocol\QueuePurgeOkFrame $frame */
        $frame = $this->write($buffer, Protocol\QueuePurgeOkFrame::class);

        return $frame->messageCount;
    }

    /**
     * @param string $queue
     * @param bool $ifUnused
     * @param bool $ifEmpty
     * @param bool $noWait
     * @return int
     */
    public function queueDelete
    (
        string $queue = '',
        bool   $ifUnused = false,
        bool   $ifEmpty = false,
        bool   $noWait = false
    ): int
    {
        $flags = [$ifUnused, $ifEmpty, $noWait];

        $buffer = (new Buffer)
            ->appendUint8(1)
            ->appendUint16($this->id)
            ->appendUint32(8 + strlen($queue))
            ->appendUint16(50)
            ->appendUint16(40)
            ->appendInt16(0)
            ->appendString($queue)
            ->appendBits($flags)
            ->appendUint8(206);

        if ($noWait) {
            $this->write($buffer);
            return 0;
        }

        $frame = $this->write($buffer, Protocol\QueueDeleteOkFrame::class, Protocol\ChannelCloseFrame::class);

        if ($frame instanceof Protocol\ChannelCloseFrame) {
            throw new ChannelException($frame->replyText, $frame->replyCode);
        }

        /** @var Protocol\QueueDeleteOkFrame $frame */
        return $frame->messageCount;
    }

    /**
     * @param string $exchange
     * @param string $exchangeType
     * @param bool $passive
     * @param bool $durable
     * @param bool $autoDelete
     * @param bool $internal
     * @param bool $noWait
     * @param array $arguments
     */
    public function exchangeDeclare
    (
        string $exchange,
        string $exchangeType = 'direct',
        bool   $passive = false,
        bool   $durable = false,
        bool   $autoDelete = false,
        bool   $internal = false,
        bool   $noWait = false,
        array  $arguments = []
    ): void
    {
        $flags = [$passive, $durable, $autoDelete, $internal, $noWait];

        $buffer = (new Buffer)
            ->appendUint16(40)
            ->appendUint16(10)
            ->appendInt16(0)
            ->appendString($exchange)
            ->appendString($exchangeType)
            ->appendBits($flags)
            ->appendTable($arguments);

        if ($noWait) {
            $this->method($buffer);
            return;
        }

        $frame = $this->method($buffer, Protocol\ExchangeDeclareOkFrame::class, Protocol\ChannelCloseFrame::class);
        if ($frame instanceof Protocol\ChannelCloseFrame) {
            throw new ChannelException($frame->replyText, $frame->replyCode);
        }
    }

    /**
     * @param string $destination
     * @param string $source
     * @param string $routingKey
     * @param bool $noWait
     * @param array $arguments
     */
    public function exchangeBind
    (
        string $destination,
        string $source,
        string $routingKey = '',
        bool   $noWait = false,
        array  $arguments = []
    ): void
    {
        $buffer = (new Buffer)
            ->appendUint16(40)
            ->appendUint16(30)
            ->appendInt16(0)
            ->appendString($destination)
            ->appendString($source)
            ->appendString($routingKey)
            ->appendBits([$noWait])
            ->appendTable($arguments);

        if( $noWait) {
            $this->method($buffer);
        } else {
            $this->method($buffer, Protocol\ExchangeBindOkFrame::class);
        }
    }

    /**
     * @param string $destination
     * @param string $source
     * @param string $routingKey
     * @param bool $noWait
     * @param array $arguments
     */
    public function exchangeUnbind
    (
        string $destination,
        string $source,
        string $routingKey = '',
        bool   $noWait = false,
        array  $arguments = []
    ): void
    {
        $buffer = (new Buffer)
            ->appendUint16(40)
            ->appendUint16(40)
            ->appendInt16(0)
            ->appendString($destination)
            ->appendString($source)
            ->appendString($routingKey)
            ->appendBits([$noWait])
            ->appendTable($arguments);

        if( $noWait ) {
            $this->method($buffer);
        } else {
            $this->method($buffer, Protocol\ExchangeUnbindOkFrame::class);
        }
    }

    /**
     * @param string $exchange
     * @param bool $unused
     * @param bool $noWait
     */
    public function exchangeDelete(string $exchange, bool $unused = false, bool $noWait = false): void
    {
        $buffer = (new Buffer)
            ->appendUint8(1)
            ->appendUint16($this->id)
            ->appendUint32(8 + \strlen($exchange))
            ->appendUint16(40)
            ->appendUint16(20)
            ->appendInt16(0)
            ->appendString($exchange)
            ->appendBits([$unused, $noWait])
            ->appendUint8(206);

        if ($noWait) {
            $this->write($buffer);
        } else {
            $this->write($buffer, Protocol\ExchangeDeleteOkFrame::class);
        }
    }

    /**
     * @param string $body
     * @param string $exchange
     * @param string $routingKey
     * @param array $headers
     * @param bool $mandatory
     * @param bool $immediate
     */
    public function doPublish
    (
        string $body,
        string $exchange = '',
        string $routingKey = '',
        array  $headers = [],
        bool   $mandatory = false,
        bool   $immediate = false
    ): void
    {
        $flags = 0;
        $contentType = '';
        $contentEncoding = '';
        $type = '';
        $replyTo = '';
        $expiration = '';
        $messageId = '';
        $correlationId = '';
        $userId = '';
        $appId = '';
        $clusterId = '';

        $deliveryMode = null;
        $priority = null;
        $timestamp = null;

        $headersBuffer = null;

        $buffer = (new Buffer)
            ->appendUint8(1)
            ->appendUint16($this->id)
            ->appendUint32(9 + \strlen($exchange) + \strlen($routingKey))
            ->appendUint16(60)
            ->appendUint16(40)
            ->appendInt16(0)
            ->appendString($exchange)
            ->appendString($routingKey)
            ->appendBits([$mandatory, $immediate])
            ->appendUint8(206);

        $size = 14;

        if (isset($headers['content-type'])) {
            $flags |= 32768;
            $contentType = (string)$headers['content-type'];
            $size += 1 + \strlen($contentType);

            unset($headers['content-type']);
        }

        if (isset($headers['content-encoding'])) {
            $flags |= 16384;
            $contentEncoding = (string)$headers['content-encoding'];
            $size += 1 + \strlen($contentEncoding);

            unset($headers['content-encoding']);
        }

        if (isset($headers['delivery-mode'])) {
            $flags |= 4096;
            $deliveryMode = (int)$headers['delivery-mode'];
            ++$size;

            unset($headers['delivery-mode']);
        }

        if (isset($headers['priority'])) {
            $flags |= 2048;
            $priority = (int)$headers['priority'];
            ++$size;

            unset($headers['priority']);
        }

        if (isset($headers['correlation-id'])) {
            $flags |= 1024;
            $correlationId = (string)$headers['correlation-id'];
            $size += 1 + \strlen($correlationId);

            unset($headers['correlation-id']);
        }

        if (isset($headers['reply-to'])) {
            $flags |= 512;
            $replyTo = (string)$headers['reply-to'];
            $size += 1 + \strlen($replyTo);

            unset($headers['reply-to']);
        }

        if (isset($headers['expiration'])) {
            $flags |= 256;
            $expiration = (string)$headers['expiration'];
            $size += 1 + \strlen($expiration);

            unset($headers['expiration']);
        }

        if (isset($headers['message-id'])) {
            $flags |= 128;
            $messageId = (string)$headers['message-id'];
            $size += 1 + \strlen($messageId);

            unset($headers['message-id']);
        }

        if (isset($headers['timestamp'])) {
            $flags |= 64;
            $timestamp = (int)$headers['timestamp'];
            $size += 8;

            unset($headers['timestamp']);
        }

        if (isset($headers['type'])) {
            $flags |= 32;
            $type = (string)$headers['type'];
            $size += 1 + \strlen($type);

            unset($headers['type']);
        }

        if (isset($headers['user-id'])) {
            $flags |= 16;
            $userId = (string)$headers['user-id'];
            $size += 1 + \strlen($userId);

            unset($headers['user-id']);
        }

        if (isset($headers['app-id'])) {
            $flags |= 8;
            $appId = (string)$headers['app-id'];
            $size += 1 + \strlen($appId);

            unset($headers['app-id']);
        }

        if (isset($headers['cluster-id'])) {
            $flags |= 4;
            $clusterId = (string)$headers['cluster-id'];
            $size += 1 + \strlen($clusterId);

            unset($headers['cluster-id']);
        }

        if (!empty($headers)) {
            $flags |= 8192;
            $headersBuffer = new Buffer;
            $headersBuffer->appendTable($headers);
            $size += $headersBuffer->size();
        }

        $buffer
            ->appendUint8(2)
            ->appendUint16($this->id)
            ->appendUint32($size)
            ->appendUint16(60)
            ->appendUint16(0)
            ->appendUint64(\strlen($body))
            ->appendUint16($flags);

        if ($flags & 32768) {
            $buffer->appendString($contentType);
        }

        if ($flags & 16384) {
            $buffer->appendString($contentEncoding);
        }

        if ($flags & 8192 && $headersBuffer !== null) {
            $buffer->append($headersBuffer);
        }

        if ($flags & 4096 && $deliveryMode !== null) {
            $buffer->appendUint8($deliveryMode);
        }

        if ($flags & 2048 && $priority !== null) {
            $buffer->appendUint8($priority);
        }

        if ($flags & 1024) {
            $buffer->appendString($correlationId);
        }

        if ($flags & 512) {
            $buffer->appendString($replyTo);
        }

        if ($flags & 256) {
            $buffer->appendString($expiration);
        }

        if ($flags & 128) {
            $buffer->appendString($messageId);
        }

        if ($flags & 64 && $timestamp !== null) {
            /** @noinspection PhpUnhandledExceptionInspection */
            $buffer->appendTimestamp(new \DateTimeImmutable(\sprintf('@%s', $timestamp)));
        }

        if ($flags & 32) {
            $buffer->appendString($type);
        }

        if ($flags & 16) {
            $buffer->appendString($userId);
        }

        if ($flags & 8) {
            $buffer->appendString($appId);
        }

        if ($flags & 4) {
            $buffer->appendString($clusterId);
        }

        $buffer->appendUint8(206);

        if (!empty($body)) {
            /* @phpstan-ignore-next-line */
            $chunks = \str_split($body, $this->properties->maxFrame());

            if ($chunks) {
                foreach ($chunks as $chunk) {
                    $buffer
                        ->appendUint8(3)
                        ->appendUint16($this->id)
                        ->appendUint32(\strlen($chunk))
                        ->append($chunk)
                        ->appendUint8(206);
                }
            }
        }

        $this->write($buffer);
    }

    /**
     * @throws ChannelException
     */
    private function throwIfClosed(): void
    {
        if (in_array($this->state, [self::STATE_CLOSED, self::STATE_ERROR])) {
            try {
                $this->cancellation()->throwIfRequested();
                throw new ChannelException('Channel #' . $this->id . ' was closed, state: ' . $this->state);
            } catch (CancelledException $e) {
                throw new ChannelException('Channel #' . $this->id . ' was closed, state: ' . $this->state . ', cancelled: ' . $e->getPrevious()?->getMessage());
            }
        }
    }

    /**
     * Write buffer and wait result
     * @param Buffer $payload
     * @param string ...$frames
     * @return AbstractFrame|null
     * @template T of Protocol\AbstractFrame
     * @psalm-param array<int, class-string<T>> $frames
     * @psalm-return T|null
     * @throws ChannelException|ProtocolException
     */
    private function method(Buffer $payload, string ...$frames): ?AbstractFrame
    {
        $this->throwIfClosed();
        try {
            if ($frames) {
                $future = $this->await(...$frames);
            }
            $this->connection->method($this->id, $payload);
            if ($frames) {
                return $future->await(new CompositeCancellation($this->cancellation(), new TimeoutCancellation(self::AWAIT_TIMEOUT)));
            }
        } catch (CancelledException $e) {
            $this->connection->onClosed = null;
            if( $e->getPrevious() instanceof ChannelException ) {
                throw $e->getPrevious();
            }
            throw new ProtocolException($e->getMessage() . ($e->getPrevious() ? '. ' . $e->getPrevious()->getMessage() : '') . '. Wait frames: ' . implode(', ', $frames ?: []));
        }
        return null;
    }

    /**
     * Write buffer and wait result
     * @param Buffer $buffer
     * @param string ...$frames
     * @return AbstractFrame|null
     * @template T of Protocol\AbstractFrame
     * @psalm-param array<int, class-string<T>> $frames
     * @psalm-return T|null
     */
    private function write(Buffer $buffer, string ...$frames): ?AbstractFrame
    {
        $this->throwIfClosed();
        try {
            if ($frames) {
                $future = $this->await(...$frames);
            }
            $this->connection->write($buffer);
            if ($frames) {
                return $future->await(new TimeoutCancellation(self::AWAIT_TIMEOUT));
            }
        } catch (CancelledException $e) {
            $this->connection->onClosed = null;
            throw new ProtocolException($e->getMessage() . ($e->getPrevious() ? '. ' . $e->getPrevious()->getMessage() : '') . '. Wait frames: ' . implode(', ', $frames ?: []));
        }
        return null;
    }

    /**
     * @template T of Protocol\AbstractFrame
     * @psalm-param array<int, class-string<T>> $frames
     * @psalm-return Future<T>
     */
    private function await(string ...$frames): Future
    {
        $deferred = new DeferredFuture();
        $ids = [];
        $unsubscribe = function () use (&$ids) {
            foreach ($ids as $frame => $id) {
                $this->connection->unsubscribe($this->id, $frame, $id);
            }
        };

        //Set callback on connection closed
        $this->connection->onClosed = static fn(string $message) => $deferred->error(ClientException::disconnected($message));

        foreach ($frames as $frame) {
            /** @psalm-var class-string<T> $frame */
            $ids[$frame] = $this->connection->subscribe($this->id, $frame, function (Protocol\AbstractFrame $frame) use ($deferred, $unsubscribe) {
                $unsubscribe();
                $this->connection->onClosed = null;
                if (!$deferred->isComplete()) {
                    /** @psalm-var T $frame */
                    $deferred->complete($frame);
                }

                return true;
            });
        }

        return $deferred->getFuture();
    }
}
