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

use Amp\DeferredFuture;
use Amp\TimeoutCancellation;
use PHPinnacle\Ridge\Channel;
use PHPinnacle\Ridge\Exception;
use PHPinnacle\Ridge\Exception\ChannelException;
use PHPinnacle\Ridge\Message;
use PHPinnacle\Ridge\Queue;
use Revolt\EventLoop;
use function Amp\async;
use function Amp\Future\await;

class ChannelTest extends AsyncTest
{
    public function testOpenNotReadyChannel(): void
    {
        $this->expectException(Exception\ChannelException::class);

        $channel = $this->client->channel();
        $this->assertNotNull($channel);

        $channel->open();

        $this->assertTrue(true);
    }

    public function testClose(): void
    {
        $channel = $this->client->channel();

        $channel->close();

        $this->assertTrue(true);
    }

    public function testCloseAlreadyClosedChannel(): void
    {
        $this->expectException(Exception\ChannelException::class);

        $channel = $this->client->channel();

        $channel->close();
        $channel->close();
    }

    public function testClosedChannel(): void
    {
        $channel = $this->client->channel();
        $channel->close();

        $this->expectException(ChannelException::class);
        $this->expectExceptionMessageMatches('/Channel #\d+ was closed/');

        $channel->queueDeclare('test_closed', true);

        $this->assertTrue($channel->cancellation()->isRequested());
    }

    public function testClosedChannelFromServer(): void
    {
        $channel = $this->client->channel();

        try {
            $channel->queueDeclare('test_closed', true);
        } catch (ChannelException $e) {
            $this->assertStringContainsString('NOT_FOUND', $e->getMessage());
        }

        $this->expectException(ChannelException::class);
        $this->expectExceptionMessageMatches('/Channel #\d+ was closed/');

        $channel->queueDeclare('test_closed');
    }

    public function testExchangeDeclare(): void
    {
        $channel = $this->client->channel();

        $channel->exchangeDeclare('test_exchange', 'direct', false, false, true);

        $this->assertFalse($channel->cancellation()->isRequested());

        $this->expectException(ChannelException::class);
        $this->expectExceptionMessageMatches('/PRECONDITION_FAILED/');

        $channel->exchangeDeclare('test_exchange');
        $this->assertTrue($channel->cancellation()->isRequested());
    }

    public function testExchangeDelete(): void
    {
        $channel = $this->client->channel();

        $channel->exchangeDeclare('test_exchange_no_ad', 'direct');

        $channel->exchangeDelete('test_exchange_no_ad');

        $this->assertTrue(true);
    }

    public function testQueueDeclare(): void
    {
        $channel = $this->client->channel();

        $queue = $channel->queueDeclare('test_queue', false, false, true, true);

        $this->assertInstanceOf(Queue::class, $queue);

        $this->assertSame('test_queue', $queue->name());
        $this->assertSame(0, $queue->messages());
        $this->assertSame(0, $queue->consumers());
    }

    public function testQueueDeclarePassive(): void
    {
        $channel = $this->client->channel();

        $this->expectException(Exception\ChannelException::class);

        $channel->queueDeclare('queue_not_exist', true);
    }

    public function testQueueBind(): void
    {
        $channel = $this->client->channel();

        $channel->exchangeDeclare('test_exchange', 'direct', false, false, true);
        $channel->queueDeclare('test_queue', false, false, true, true);

        $channel->queueBind('test_queue', 'test_exchange');
    }

    public function testQueueUnbind(): void
    {
        $channel = $this->client->channel();

        $channel->exchangeDeclare('test_exchange', 'direct', false, false, true);
        $channel->queueDeclare('test_queue', false, false, true, true);
        $channel->queueBind('test_queue', 'test_exchange');

        $channel->queueUnbind('test_queue', 'test_exchange');
    }

    public function testQueuePurge(): void
    {
        $channel = $this->client->channel();

        $channel->queueDeclare('test_queue', false, false, true, true);
        $channel->publish('test', '', 'test_queue');
        $channel->publish('test', '', 'test_queue');

        $messages = $channel->queuePurge('test_queue');

        $this->assertEquals(2, $messages);
    }

    public function testQueueDelete(): void
    {
        $channel = $this->client->channel();

        $channel->queueDeclare('test_queue_no_ad');
        $channel->publish('test', '', 'test_queue_no_ad');

        $messages = $channel->queueDelete('test_queue_no_ad');

        $this->assertEquals(1, $messages);
    }

    public function testQueueDeleteNotEmpty(): void
    {
        $channel = $this->client->channel();
        $channel->queueDeclare('test_message', false, true);
        $channel->publish('test message', '', 'test_message');

        $this->expectException(ChannelException::class);
        $this->expectExceptionMessageMatches('/PRECONDITION_FAILED/');

        $channel->queueDelete('test_message', false, true);
    }

    public function testQueueDeleteNotEmptyNotAck(): void
    {
        $channel = $this->client->channel();
        $channel->queueDeclare('test_message', false, true);
        $channel->publish('test message', '', 'test_message');
        $message = $channel->get('test_message');

        $this->expectException(ChannelException::class);
        $this->expectExceptionMessageMatches('/PRECONDITION_FAILED/');

        $channel->queueDelete('test_message', false, true);
    }

    public function testPublish(): void
    {
        $channel = $this->client->channel();

        $this->assertNull($channel->publish('test publish'));
    }

    public function testMandatoryPublish(): void
    {
        $channel = $this->client->channel();

        $deferred = new DeferredFuture();
        $watcher = EventLoop::delay(100, function () use ($deferred) {
            $deferred->complete(false);
        });

        $channel->events()->onReturn(function (Message $message) use ($deferred, $watcher) {
            $this->assertSame($message->content, '.');
            $this->assertSame($message->exchange, '');
            $this->assertSame($message->routingKey, '404');
            $this->assertSame($message->headers, []);
            $this->assertNull($message->consumerTag);
            $this->assertNull($message->deliveryTag);
            $this->assertFalse($message->redelivered);
            $this->assertTrue($message->returned);

            EventLoop::cancel($watcher);

            $deferred->complete(true);
        });

        $channel->publish('.', '', '404', [], true);

        $this->assertTrue($deferred->getFuture()->await(), 'Mandatory return event not received!');
    }

    public function testImmediatePublish(): void
    {
        $properties = $this->client->properties();

        // RabbitMQ 3 doesn't support "immediate" publish flag.
        if ($properties->product() === 'RabbitMQ' && version_compare($properties->version(), '3.0', '>')) {
            $this->client->disconnect();
            return;
        }

        $channel = $this->client->channel();

        $deferred = new DeferredFuture();
        $watcher = EventLoop::delay(100, function () use ($deferred) {
            $deferred->complete(false);
        });

        $channel->events()->onReturn(function (Message $message) use ($deferred, $watcher) {
            $this->assertTrue($message->returned);

            EventLoop::cancel($watcher);

            $deferred->complete(true);
        });

        $channel->queueDeclare('test_queue', false, false, true, true);
        $channel->publish('.', '', 'test_queue', [], false, true);

        $this->assertTrue($deferred->getFuture()->await(), 'Immediate return event not received!');
    }

    public function testConsume(): void
    {
        $channel = $this->client->channel();

        $channel->queueDeclare('test_queue', false, false, true, true);
        $channel->publish('hi', '', 'test_queue');

        $deferred = new DeferredFuture();
        $tag = $channel->consume(function (Message $message) use (&$tag, $deferred) {
            $this->assertEquals('hi', $message->content);
            $deferred->complete();
        }, 'test_queue', '', true);

        $deferred->getFuture()->await(new TimeoutCancellation(1));
    }

    public function testConsumeNoAck() {
        $channel = $this->client->channel();

        $channel->queueDeclare('test_queue', false, false, true);
        $channel->publish('hi', '', 'test_queue');

        $deferred = new DeferredFuture();
        $tag = $channel->consume(function (Message $message) use (&$tag, $deferred) {
            $this->assertEquals('hi', $message->content);
            $deferred->complete();
        }, 'test_queue', '', true, true);

        $deferred->getFuture()->await(new TimeoutCancellation(1));
    }

    public function testConsumeNoWait() {
        $channel = $this->client->channel();

        $channel->queueDeclare('test_queue', false, false, true);
        $channel->publish('hi', '', 'test_queue');

        $this->expectException(Exception\ProtocolException::class);
        $channel->consume(fn() => 1, 'test_queue', '', true, true, false, true);
    }

    public function testConsumeNoAckWithTag() {
        $channel = $this->client->channel();

        $channel->queueDeclare('test_queue', false, false, true);
        $channel->publish('hi', '', 'test_queue');

        $deferred = new DeferredFuture();
        $tag = 'test_consumer';
        $this->assertEquals($tag, $channel->consume(function (Message $message) use (&$tag, $deferred) {
            $this->assertEquals('hi', $message->content);
            $this->assertEquals($tag, $message->consumerTag);
            $deferred->complete();
        }, 'test_queue', $tag, true, true));

        $deferred->getFuture()->await(new TimeoutCancellation(1));
    }

    public function testDoubleConsume(): void
    {
        $channel = $this->client->channel();

        $channel->queueDeclare('test_queue', false, false, true, true);

        $this->expectException(Exception\ClientException::class);
        $this->expectExceptionMessageMatches('/NOT_ALLOWED/');

        $channel->consume(fn() => null, 'test_queue', 'test_comsumer', true);
        $channel->consume(fn() => null, 'test_queue', 'test_comsumer', true);
    }

    public function testConsumeNotExistsQueue(): void
    {
        $channel = $this->client->channel();

        $this->expectException(Exception\ChannelException::class);
        $this->expectExceptionMessageMatches('/NOT_FOUND/');

        $channel->consume(fn() => null, 'queue_not_exist');
    }

    public function testCancel(): void
    {
        $channel = $this->client->channel();

        $channel->queueDeclare('test_queue', false, false, true, true);
        $channel->publish('hi', '', 'test_queue');

        $tag = $channel->consume(function (Message $message) {
        }, 'test_queue', false, true);

        $channel->cancel($tag);
    }

    public function testHeaders(): void
    {
        $channel = $this->client->channel();

        $channel->queueDeclare('test_queue', false, false, true, true);
        $channel->publish('<b>hi html</b>', '', 'test_queue', [
            'content-type' => 'text/html',
            'custom' => 'value',
        ]);

        $deferred = new DeferredFuture();
        $channel->consume(function (Message $message) use ($deferred) {
            $this->assertEquals('text/html', $message->header('content-type'));
            $this->assertEquals('value', $message->header('custom'));
            $this->assertEquals('<b>hi html</b>', $message->content);

            $deferred->complete();
        }, 'test_queue', false, true);

        $deferred->getFuture()->await(new TimeoutCancellation(1));
    }

    public function testGet(): void
    {
        $channel = $this->client->channel();

        $channel->queueDeclare('get_test', false, false, false, true);

        $channel->publish('.', '', 'get_test');

        /** @var Message $message1 */
        $message1 = $channel->get('get_test', true);

        $this->assertNotNull($message1);
        $this->assertInstanceOf(Message::class, $message1);
        $this->assertEquals('', $message1->exchange);
        $this->assertEquals('.', $message1->content);
        $this->assertEquals('get_test', $message1->routingKey);
        $this->assertEquals(1, $message1->deliveryTag);
        $this->assertNull($message1->consumerTag);
        $this->assertFalse($message1->redelivered);
        $this->assertIsArray($message1->headers);

        $this->assertNull($channel->get('get_test', true));

        $channel->publish('..', '', 'get_test');

        /** @var Message $message2 */
        $message2 = $channel->get('get_test');

        $this->assertNotNull($message2);
        $this->assertEquals(2, $message2->deliveryTag);
        $this->assertFalse($message2->redelivered);

        $this->client->disconnect();
        $this->client->connect();

        $channel = $this->client->channel();

        /** @var Message $message3 */
        $message3 = $channel->get('get_test');

        $this->assertNotNull($message3);
        $this->assertInstanceOf(Message::class, $message3);
        $this->assertEquals('', $message3->exchange);
        $this->assertEquals('..', $message3->content);
        $this->assertTrue($message3->redelivered);

        $channel->ack($message3);
    }

    public function testAck(): void
    {
        $channel = $this->client->channel();

        $channel->queueDeclare('test_queue', false, false, true, true);
        $channel->publish('.', '', 'test_queue');

        /** @var Message $message */
        $message = $channel->get('test_queue');
        $channel->ack($message);
    }

    public function testNack(): void
    {
        $channel = $this->client->channel();

        $channel->queueDeclare('test_queue', false, false, true, true);
        $channel->publish('.', '', 'test_queue');

        /** @var Message $message */
        $message = $channel->get('test_queue');

        $this->assertNotNull($message);
        $this->assertFalse($message->redelivered);

        $channel->nack($message);

        $message = $channel->get('test_queue');

        $this->assertNotNull($message);
        $this->assertTrue($message->redelivered);

        $channel->nack($message, false, false);

        $this->assertNull($channel->get('test_queue'));
    }

    public function testReject(): void
    {
        $channel = $this->client->channel();

        $channel->queueDeclare('test_queue', false, false, true, true);
        $channel->publish('.', '', 'test_queue');

        $message = $channel->get('test_queue');

        $this->assertNotNull($message);
        $this->assertFalse($message->redelivered);

        $channel->reject($message);

        $message = $channel->get('test_queue');

        $this->assertNotNull($message);
        $this->assertTrue($message->redelivered);

        $channel->reject($message, false);

        $this->assertNull($channel->get('test_queue'));
    }

    public function testRecover(): void
    {
        $channel = $this->client->channel();

        $channel->queueDeclare('test_queue', false, false, true, true);
        $channel->publish('.', '', 'test_queue');

        $message = $channel->get('test_queue');

        $this->assertNotNull($message);
        $this->assertFalse($message->redelivered);

        $channel->recover(true);

        $message = $channel->get('test_queue');

        $this->assertNotNull($message);
        $this->assertTrue($message->redelivered);

        $channel->ack($message);
    }

    public function testBigMessage(): void
    {
        $channel = $this->client->channel();

        $channel->queueDeclare('test_queue', false, false, true, true);

        $body = \str_repeat('a', 10 << 20); // 10 MiB

        $channel->publish($body, '', 'test_queue');

        $deferred = new DeferredFuture();
        $channel->consume(function (Message $message, Channel $channel) use ($deferred, $body) {
            $this->assertEquals(\strlen($body), \strlen($message->content));

            $channel->ack($message);
            $deferred->complete();
        }, 'test_queue');

        $deferred->getFuture()->await(new TimeoutCancellation(1));
    }

    public function testGetDouble(): void
    {
        $this->expectException(Exception\ChannelException::class);

        $channel = $this->client->channel();

        $channel->queueDeclare('get_test_double', false, false, true, true);
        $channel->publish('.', '', 'get_test_double');

        try {
            await([
                async(fn() => $channel->get('get_test_double')),
                async(fn() => $channel->get('get_test_double')),
            ]);
        } finally {
            $channel->queueDelete('get_test_double');
        }
    }

    public function testEmptyMessage(): void
    {
        $channel = $this->client->channel();

        $channel->queueDeclare('empty_body_message_test', false, false, true, true);
        $channel->publish('', '', 'empty_body_message_test');

        /** @var Message $message */
        $message = $channel->get('empty_body_message_test', true);

        $this->assertNotNull($message);
        $this->assertEquals('', $message->content);

        $count = 0;

        $deferred = new DeferredFuture();
        $channel->consume(function (Message $message, Channel $channel) use ($deferred, &$count) {
            $this->assertEmpty($message->content);

            $channel->ack($message);

            if (++$count === 2) {
                $deferred->complete();
            }
        }, 'empty_body_message_test');

        $channel->publish('', '', 'empty_body_message_test');
        $channel->publish('', '', 'empty_body_message_test');

        $deferred->getFuture()->await();
    }

    public function testTxs(): void
    {
        $channel = $this->client->channel();

        $channel->queueDeclare('tx_test', false, false, true, true);

        $channel->txSelect();
        $channel->publish('.', '', 'tx_test');
        $channel->txCommit();

        /** @var Message $message */
        $message = $channel->get('tx_test', true);

        $this->assertNotNull($message);
        $this->assertInstanceOf(Message::class, $message);
        $this->assertEquals('.', $message->content);

        $channel->publish('..', '', 'tx_test');
        $channel->txRollback();

        $nothing = $channel->get('tx_test', true);

        $this->assertNull($nothing);
    }

    public function testTxSelectCannotBeCalledMultipleTimes(): void
    {
        $this->expectException(Exception\ChannelException::class);

        $channel = $this->client->channel();

        $channel->txSelect();
        $channel->txSelect();
    }

    public function testTxCommitCannotBeCalledUnderNotTransactionMode(): void
    {
        $this->expectException(Exception\ChannelException::class);

        $channel = $this->client->channel();

        $channel->txCommit();
    }

    public function testTxRollbackCannotBeCalledUnderNotTransactionMode(): void
    {
        $this->expectException(Exception\ChannelException::class);

        $channel = $this->client->channel();

        $channel->txRollback();
    }

    public function testConfirmMode(): void
    {
        $channel = $this->client->channel();
        $channel->events()->onAck(function (int $deliveryTag, bool $multiple) {
            $this->assertEquals($deliveryTag, 1);
            $this->assertFalse($multiple);
        });

        $channel->confirmSelect();

        $deliveryTag = $channel->publish('.');

        $this->assertEquals($deliveryTag, 1);
    }
}
