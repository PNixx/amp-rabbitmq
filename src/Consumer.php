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

use PHPinnacle\Ridge\Exception\ConsumerException;
use function Amp\async;

final class Consumer
{
    /**
     * @var callable[]
     * @psalm-var array<string, callable>
     */
    private array $listeners = [];

    public function __construct(private readonly Channel $channel, private readonly MessageReceiver $receiver)
    {
    }

    public function start(): void
    {
        $this->receiver->onMessage(function (Message $message) {
            if (!$tag = $message->consumerTag) {
                return;
            }

            if (!isset($this->listeners[$tag])) {
                throw new ConsumerException('Not listener for tag ' . $tag);
            }

            async(fn($fn) => $fn($message, $this->channel), $this->listeners[$tag]);
        });
    }

    public function stop(): void
    {
        $this->listeners = [];
    }

    public function subscribe(string $tag, callable $listener): void
    {
        $this->listeners[$tag] = $listener;
    }

    public function cancel(string $tag): void
    {
        unset($this->listeners[$tag]);
    }
}
