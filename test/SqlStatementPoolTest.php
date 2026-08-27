<?php declare(strict_types=1);

namespace Amp\Sql\Common\Test;

use Amp\PHPUnit\AsyncTestCase;
use Amp\Sql\Common\SqlStatementPool;
use Amp\Sql\Common\Test\Stub\StubSqlPooledResult;
use Amp\Sql\Common\Test\Stub\StubSqlResult;
use Amp\Sql\SqlConnectionPool;
use Amp\Sql\SqlResult;
use Amp\Sql\SqlStatement;
use function Amp\delay;

class SqlStatementPoolTest extends AsyncTestCase
{
    public function testActiveStatementsRemainAfterTimeout()
    {
        $pool = $this->createMock(SqlConnectionPool::class);
        $pool->method('isClosed')
            ->willReturn(false);
        $pool->method('getIdleTimeout')
            ->willReturn(60);

        $statement = $this->createMock(SqlStatement::class);
        $statement->method('isClosed')
            ->willReturn(false);
        $statement->method('getQuery')
            ->willReturn('SELECT 1');
        $statement->method('getLastUsedAt')
            ->willReturn(\time());
        $statement->expects($this->once())
            ->method('execute');

        $statementPool = $this->getMockBuilder(SqlStatementPool::class)
            ->setConstructorArgs([$pool, 'SELECT 1', $this->createCallback(1, fn () => $statement)])
            ->getMockForAbstractClass();

        self::assertFalse($statementPool->isClosed());
        self::assertSame(\time(), $statementPool->getLastUsedAt());

        delay(1.5); // Give timeout watcher enough time to execute.

        $statementPool->execute();

        self::assertFalse($statementPool->isClosed());
        self::assertSame(\time(), $statementPool->getLastUsedAt());
    }

    public function testIdleStatementsRemovedAfterTimeout()
    {
        $pool = $this->createMock(SqlConnectionPool::class);
        $pool->method('isClosed')
            ->willReturn(false);
        $pool->method('getIdleTimeout')
            ->willReturn(1);

        $createStatement = function (): SqlStatement {
            $statement = $this->createMock(SqlStatement::class);
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

        $statementPool = $this->getMockBuilder(SqlStatementPool::class)
            ->setConstructorArgs([$pool, 'SELECT 1', $this->createCallback(2, $createStatement)])
            ->getMockForAbstractClass();

        self::assertFalse($statementPool->isClosed());
        self::assertSame(\time(), $statementPool->getLastUsedAt());

        $statementPool->execute();

        delay(1.5); // Give timeout watcher enough time to execute.

        $statementPool->execute();

        self::assertFalse($statementPool->isClosed());
        self::assertSame(\time(), $statementPool->getLastUsedAt());
    }

    private function createStatementPool(SqlConnectionPool $pool, \Closure $prepare): SqlStatementPool
    {
        return new class($pool, 'SELECT 1', $prepare) extends SqlStatementPool {
            protected function createResult(SqlResult $result, \Closure $release): SqlResult
            {
                return new StubSqlPooledResult($result, $release);
            }
        };
    }

    public function testDeclinedStatementIsClosedSoItsConnectionIsReleased()
    {
        $pool = $this->createMock(SqlConnectionPool::class);
        $pool->method('isClosed')
            ->willReturn(false);
        $pool->method('getIdleTimeout')
            ->willReturn(60);
        $pool->method('getConnectionLimit')
            ->willReturn(1);
        $pool->method('getConnectionCount')
            ->willReturn(1);
        $pool->method('getIdleConnectionCount')
            ->willReturn(0);

        $createStatement = function (): SqlStatement {
            $statement = $this->createMock(SqlStatement::class);
            $statement->method('isClosed')
                ->willReturn(false);
            $statement->method('getQuery')
                ->willReturn('SELECT 1');
            $statement->method('getLastUsedAt')
                ->willReturn(\time());
            $statement->method('execute')
                ->willReturn(new StubSqlResult([]));
            $statement->expects($this->once())
                ->method('close');

            return $statement;
        };

        // The pool is saturated (limit 1, all connections busy), so push() declines to
        // retain the statement. It must then close the statement and drop its reference,
        // otherwise the checked-out connection is never returned and the pool deadlocks
        // on the next execute() while the first result is still referenced.
        $statementPool = $this->createStatementPool($pool, $this->createCallback(2, $createStatement));

        $result = $statementPool->execute();
        \iterator_to_array($result);

        delay(0.1); // Allow the queued release to run.

        // The first result remains referenced; a fresh statement must be prepared.
        $secondResult = $statementPool->execute();
        \iterator_to_array($secondResult);

        delay(0.1);
    }

    public function testStatementIsRetainedWhenThePoolHasCapacity()
    {
        $pool = $this->createMock(SqlConnectionPool::class);
        $pool->method('isClosed')
            ->willReturn(false);
        $pool->method('getIdleTimeout')
            ->willReturn(60);
        $pool->method('getConnectionLimit')
            ->willReturn(20);
        $pool->method('getConnectionCount')
            ->willReturn(2);
        $pool->method('getIdleConnectionCount')
            ->willReturn(1);

        $statement = $this->createMock(SqlStatement::class);
        $statement->method('isClosed')
            ->willReturn(false);
        $statement->method('getQuery')
            ->willReturn('SELECT 1');
        $statement->method('getLastUsedAt')
            ->willReturn(\time());
        $statement->method('execute')
            ->willReturnCallback(static fn () => new StubSqlResult([]));
        $statement->expects($this->never())
            ->method('close');

        $statementPool = $this->createStatementPool($pool, $this->createCallback(1, fn () => $statement));

        $result = $statementPool->execute();
        \iterator_to_array($result);

        delay(0.1); // Allow the queued release to run.

        // The retained statement is reused instead of preparing a new one.
        $secondResult = $statementPool->execute();
        \iterator_to_array($secondResult);

        delay(0.1);
    }
}
