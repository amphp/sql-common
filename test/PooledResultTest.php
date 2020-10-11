<?php

namespace Amp\Sql\Common\Test;

use Amp\PHPUnit\AsyncTestCase;
use Amp\Sql\Common\PooledResult;
use Amp\Sql\Result;
use function Amp\delay;

class PooledResultTest extends AsyncTestCase
{
    public function testIdleConnectionsRemovedAfterTimeout()
    {
        $invoked = false;

        $release = function () use (&$invoked) {
            $invoked = true;
        };

        $secondResult = $this->createMock(Result::class);
        $secondResult->method('continue')
            ->willReturnOnConsecutiveCalls(['column' => 'value'], null);
        $secondResult->method('getNextResult')
            ->willReturn(null);

        $firstResult = $this->createMock(Result::class);
        $firstResult->method('continue')
            ->willReturnOnConsecutiveCalls(['column' => 'value'], null);
        $firstResult->method('getNextResult')
            ->willReturn($secondResult);

        $result = new PooledResult($firstResult, $release);

        $this->assertSame(['column' => 'value'], $result->continue());

        $this->assertFalse($invoked);

        $this->assertNull($result->continue());

        $this->assertFalse($invoked); // Next result set available.

        $result = $result->getNextResult();

        $this->assertSame(['column' => 'value'], $result->continue());

        $this->assertFalse($invoked);

        $this->assertNull($result->continue());

        delay(0); // Tick event loop to resolve promise fetching next row.

        $this->assertTrue($invoked); // No next result set, so release callback invoked.
    }
}
