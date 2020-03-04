<?php

namespace Amp\Sql\Common\Test;

use Amp\PHPUnit\AsyncTestCase;
use Amp\Sql\Common\PooledResultSet;
use Amp\Sql\ResultSet;
use Amp\Success;

class PooledResultSetTest extends AsyncTestCase
{
    public function testIdleConnectionsRemovedAfterTimeout(): \Generator
    {
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
    }
}
