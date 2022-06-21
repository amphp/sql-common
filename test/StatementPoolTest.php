<?php

namespace Amp\Sql\Common\Test;

use Amp\PHPUnit\AsyncTestCase;
use Amp\Sql\Common\StatementPool;
use Amp\Sql\Pool;
use Amp\Sql\Statement;
use function Amp\delay;

class StatementPoolTest extends AsyncTestCase
{
    public function testActiveStatementsRemainAfterTimeout()
    {
        $pool = $this->createMock(Pool::class);
        $pool->method('isClosed')
            ->willReturn(false);
        $pool->method('getIdleTimeout')
            ->willReturn(60);

        $statement = $this->createMock(Statement::class);
        $statement->method('isClosed')
            ->willReturn(false);
        $statement->method('getQuery')
            ->willReturn('SELECT 1');
        $statement->method('getLastUsedAt')
            ->willReturn(\time());
        $statement->expects($this->once())
            ->method('execute');

        $statementPool = new StatementPool($pool, 'SELECT 1', $this->createCallback(1, fn () => $statement));

        $this->assertFalse($statementPool->isClosed());
        $this->assertSame(\time(), $statementPool->getLastUsedAt());

        delay(1.5); // Give timeout watcher enough time to execute.

        $statementPool->execute();

        $this->assertFalse($statementPool->isClosed());
        $this->assertSame(\time(), $statementPool->getLastUsedAt());
    }

    public function testIdleStatementsRemovedAfterTimeout()
    {
        $pool = $this->createMock(Pool::class);
        $pool->method('isClosed')
            ->willReturn(false);
        $pool->method('getIdleTimeout')
            ->willReturn(1);

        $createStatement = function (): Statement {
            $statement = $this->createMock(Statement::class);
            $statement->method('isClosed')
                ->willReturn(false);
            $statement->method('getQuery')
                ->willReturn('SELECT 1');
            $statement->method('getLastUsedAt')
                ->willReturn(\time());
            $statement->expects($this->once())
                ->method('execute');

            return $statement;
        };

        $statementPool = new StatementPool($pool, 'SELECT 1', $this->createCallback(2, $createStatement));

        $this->assertFalse($statementPool->isClosed());
        $this->assertSame(\time(), $statementPool->getLastUsedAt());

        $statementPool->execute();

        delay(1.5); // Give timeout watcher enough time to execute.

        $statementPool->execute();

        $this->assertFalse($statementPool->isClosed());
        $this->assertSame(\time(), $statementPool->getLastUsedAt());
    }
}
