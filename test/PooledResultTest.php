<?php

namespace Amp\Sql\Common\Test;

use Amp\PHPUnit\AsyncTestCase;
use Amp\Sql\Common\PooledResult;
use function Amp\delay;

class PooledResultTest extends AsyncTestCase
{
    public function testIdleConnectionsRemovedAfterTimeout()
    {
        $invoked = false;

        $release = function () use (&$invoked) {
            $invoked = true;
        };

        $secondResult = $this->createMock(PooledResult::class);
        $secondResult->method('getIterator')
            ->willReturn(new \ArrayIterator([['column' => 'value']]));
        $secondResult->method('getNextResult')
            ->willReturn(null);

        $firstResult = $this->createMock(PooledResult::class);
        $firstResult->method('getIterator')
            ->willReturn(new \ArrayIterator([['column' => 'value']]));
        $firstResult->method('getNextResult')
            ->willReturn($secondResult);

        $result = new PooledResult($firstResult, $release);
        $iterator = $result->getIterator();

        $this->assertSame(['column' => 'value'], $iterator->current());

        $this->assertFalse($invoked);

        $iterator->next();
        $this->assertFalse($iterator->valid());

        $this->assertFalse($invoked); // Next result set available.

        $result = $result->getNextResult();
        $iterator = $result->getIterator();

        $this->assertSame(['column' => 'value'], $iterator->current());

        $iterator->next();
        $this->assertFalse($iterator->valid());

        delay(0); // Tick event loop to resolve promise fetching next row.

        $this->assertTrue($invoked); // No next result set, so release callback invoked.
    }
}
