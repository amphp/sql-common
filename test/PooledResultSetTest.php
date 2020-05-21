<?php

namespace Amp\Sql\Common\Test;

use Amp\PHPUnit\AsyncTestCase;
use Amp\Sql\Common\PooledResultSet;
use Amp\Sql\ResultSet;
use Amp\Success;

class MockPooledResultSet extends PooledResultSet
{
    protected function createNewInstanceFrom(ResultSet $result, callable $release): PooledResultSet
    {
        return new self($result, $release);
    }
}

class PooledResultSetTest extends AsyncTestCase
{
    public function testIdleConnectionsRemovedAfterTimeout(): \Generator
    {
        $invoked = false;

        $release = function () use (&$invoked) {
            $invoked = true;
        };

        $secondResult = $this->createMock(ResultSet::class);
        $secondResult->method('continue')
            ->willReturnOnConsecutiveCalls(new Success(['column' => 'value']), new Success(null));
        $secondResult->method('getNextResultSet')
            ->willReturn(new Success(null));

        $firstResult = $this->createMock(ResultSet::class);
        $firstResult->method('continue')
            ->willReturnOnConsecutiveCalls(new Success(['column' => 'value']), new Success(null));
        $firstResult->method('getNextResultSet')
            ->willReturn(new Success($secondResult));

        $result = new MockPooledResultSet($firstResult, $release);

        $this->assertSame(['column' => 'value'], yield $result->continue());

        $this->assertFalse($invoked);

        $this->assertNull(yield $result->continue());

        $this->assertFalse($invoked); // Next result set available.

        $result = yield $result->getNextResultSet();

        $this->assertSame(['column' => 'value'], yield $result->continue());

        $this->assertFalse($invoked);

        $this->assertNull(yield $result->continue());

        $this->assertTrue($invoked); // No next result set, so release callback invoked.
    }
}
