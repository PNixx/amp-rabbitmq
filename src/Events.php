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

use function Amp\async;

final class Events
{
    public function __construct(private readonly Channel $channel, private readonly MessageReceiver $receiver)
    {
    }

    public function onAck(callable $listener): self
    {
        $this->onFrame(Protocol\BasicAckFrame::class, $listener);

        return $this;
    }

    public function onNack(callable $listener): self
    {
        $this->onFrame(Protocol\BasicNackFrame::class, $listener);

        return $this;
    }

    public function onReturn(callable $listener): self
    {
        $this->receiver->onMessage(function (Message $message) use ($listener) {
            if (!$message->returned) {
                return;
            }

            async(fn() => $listener($message, $this->channel));
        });

        return $this;
    }

    /**
     * @psalm-param class-string<Protocol\AbstractFrame> $frame
     */
    private function onFrame(string $frame, callable $callback): void
    {
        $this->receiver->onFrame($frame, function (Protocol\AcknowledgmentFrame $frame) use ($callback) {
            async(fn() => $callback($frame->deliveryTag, $frame->multiple, $this->channel));
        });
    }
}
