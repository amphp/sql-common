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
        $pool->method('isAlive')
            ->willReturn(true);
        $pool->method('getIdleTimeout')
            ->willReturn(60);

        $statement = $this->createMock(Statement::class);
        $statement->method('isAlive')
            ->willReturn(true);
        $statement->method('getQuery')
            ->willReturn('SELECT 1');
        $statement->method('getLastUsedAt')
            ->willReturn(\time());
        $statement->expects($this->once())
            ->method('execute');

        /** @var StatementPool $statementPool */
        $statementPool = $this->getMockBuilder(StatementPool::class)
            ->setConstructorArgs([$pool, 'SELECT 1', $this->createCallback(1, fn() => $statement)])
            ->getMockForAbstractClass();

        $statementPool->method('prepare')
            ->willReturn($statement);

        $this->assertTrue($statementPool->isAlive());
        $this->assertSame(\time(), $statementPool->getLastUsedAt());

        delay(1.5); // Give timeout watcher enough time to execute.

        $statementPool->execute();

        $this->assertTrue($statementPool->isAlive());
        $this->assertSame(\time(), $statementPool->getLastUsedAt());
    }

    public function testIdleStatementsRemovedAfterTimeout()
    {
        $pool = $this->createMock(Pool::class);
        $pool->method('isAlive')
            ->willReturn(true);
        $pool->method('getIdleTimeout')
            ->willReturn(1);

        $createStatement = function (): Statement {
            $statement = $this->createMock(Statement::class);
            $statement->method('isAlive')
                ->willReturn(true);
            $statement->method('getQuery')
                ->willReturn('SELECT 1');
            $statement->method('getLastUsedAt')
                ->willReturn(\time());
            $statement->expects($this->once())
                ->method('execute');

            return $statement;
        };

        /** @var StatementPool $statementPool */
        $statementPool = $this->getMockBuilder(StatementPool::class)
            ->setConstructorArgs([$pool, 'SELECT 1', $this->createCallback(2, $createStatement)])
            ->getMockForAbstractClass();

        $statementPool->method('prepare')
            ->willReturnCallback(function (Statement $statement): Statement {
                return $statement;
            });

        $this->assertTrue($statementPool->isAlive());
        $this->assertSame(\time(), $statementPool->getLastUsedAt());

        $statementPool->execute();

        delay(1.5); // Give timeout watcher enough time to execute.

        $statementPool->execute();

        $this->assertTrue($statementPool->isAlive());
        $this->assertSame(\time(), $statementPool->getLastUsedAt());
    }
}
