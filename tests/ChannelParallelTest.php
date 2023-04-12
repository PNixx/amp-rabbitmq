<?php

namespace PHPinnacle\Ridge\Tests;

use PHPinnacle\Ridge\Channel;
use PHPinnacle\Ridge\Exception\ChannelException;
use PHPinnacle\Ridge\Queue;
use function Amp\async;
use function Amp\Future\awaitAll;

class ChannelParallelTest extends AsyncTest
{

    public function testDeclare()
    {
        $iterations = 1000;
        $valid = 555;
        /** @var Channel[] $channels */
        $channels = [];
        try {
            //Prepare channels
            for($i = 0; $i < $iterations; $i++) {
                $channels[$i] = $this->client->channel();
            }

            //Declare queue
            for($i = 0; $i < $valid; $i++) {
                $channels[$i]->queueDeclare('test' . $i);
            }

            //Test
            $promises = [];
            for($i = 0; $i < $iterations; $i++) {
                $promises[$i] = async(function() use ($channels, $i) {
                    try {
                        return $channels[$i]->queueDeclare('test' . $i, true);
                    } catch (ChannelException $e) {
                        return $this->client->channel()->queueDeclare('test' . $i);
                    }
                });
            }
            [$errors, $success] = awaitAll($promises);

            for($i = 0; $i < $iterations; $i++ ) {
                $this->assertInstanceOf(Queue::class, $success[$i]);
            }

        } finally {
            //Declare queue
            for($i = 0; $i < $iterations; $i++) {
                try {
                    $channel = $this->client->channel();
                    $channel->queueDelete('test' . $i);
                    $channel->close();
                } catch (\Throwable) {
                }
            }
        }
    }
}
