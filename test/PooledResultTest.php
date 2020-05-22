<?php

namespace Amp\Sql\Common\Test;

use Amp\PHPUnit\AsyncTestCase;
use Amp\Sql\Common\PooledResult;
use Amp\Sql\Result;
use Amp\Success;

class PooledResultTest extends AsyncTestCase
{
    public function testIdleConnectionsRemovedAfterTimeout(): \Generator
    {
        $invoked = false;

        $release = function () use (&$invoked) {
            $invoked = true;
        };

        $secondResult = $this->createMock(Result::class);
        $secondResult->method('continue')
            ->willReturnOnConsecutiveCalls(new Success(['column' => 'value']), new Success(null));
        $secondResult->method('getNextResult')
            ->willReturn(new Success(null));

        $firstResult = $this->createMock(Result::class);
        $firstResult->method('continue')
            ->willReturnOnConsecutiveCalls(new Success(['column' => 'value']), new Success(null));
        $firstResult->method('getNextResult')
            ->willReturn(new Success($secondResult));

        $result = new PooledResult($firstResult, $release);

        $this->assertSame(['column' => 'value'], yield $result->continue());

        $this->assertFalse($invoked);

        $this->assertNull(yield $result->continue());

        $this->assertFalse($invoked); // Next result set available.

        $result = yield $result->getNextResult();

        $this->assertSame(['column' => 'value'], yield $result->continue());

        $this->assertFalse($invoked);

        $this->assertNull(yield $result->continue());

        $this->assertTrue($invoked); // No next result set, so release callback invoked.
    }
}
