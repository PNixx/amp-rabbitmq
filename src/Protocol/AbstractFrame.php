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

namespace PHPinnacle\Ridge\Protocol;

abstract class AbstractFrame
{
    public function __construct(
        public ?int    $type = null,
        public ?int    $channel = null,
        public ?int    $size = null,
        public ?string $payload = null
    )
    {
    }
}
