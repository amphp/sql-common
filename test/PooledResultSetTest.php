<?php

namespace Amp\Sql\Common\Test;

use Amp\Loop;
use Amp\Sql\Common\PooledResultSet;
use Amp\Sql\ResultSet;
use Amp\Success;
use PHPUnit\Framework\TestCase;

class PooledResultSetTest extends TestCase
{
    public function testIdleConnectionsRemovedAfterTimeout()
    {
        Loop::run(function () {
            $invoked = false;

            $release = function () use (&$invoked) {
                $invoked = true;
            };

            $result = $this->createMock(ResultSet::class);
            $result->method('advance')
                ->willReturnOnConsecutiveCalls(new Success(true), new Success(false));

            $result = new PooledResultSet($result, $release);

            $this->assertTrue(yield $result->advance());

            $this->assertFalse($invoked);

            $this->assertFalse(yield $result->advance());

            $this->assertTrue($invoked);
        });
    }
}
