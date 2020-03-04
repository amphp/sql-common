<?php

namespace Amp\Sql\Common\Test;

use Amp\Delayed;
use Amp\PHPUnit\AsyncTestCase;
use Amp\Promise;
use Amp\Sql\CommandResult;
use Amp\Sql\Common\ConnectionPool;
use Amp\Sql\ConnectionConfig;
use Amp\Sql\Connector;
use Amp\Sql\Link;
use Amp\Success;

class ConnectionPoolTest extends AsyncTestCase
{
    public function testInvalidMaxConnections()
    {
        $this->expectException(\Error::class);
        $this->expectExceptionMessage('Pool must contain at least one connection');

        $this->getMockBuilder(ConnectionPool::class)
            ->setConstructorArgs([$this->createMock(ConnectionConfig::class), 0])
            ->getMock();
    }

    private function createConnector(): Connector
    {
        $now = \time();

        $connector = $this->createMock(Connector::class);
        $connector->method('connect')
            ->willReturnCallback(function () use ($now): Promise {
                $link = $this->createMock(Link::class);
                $link->method('getLastUsedAt')
                    ->willReturn($now);

                $link->method('isAlive')
                    ->willReturn(true);

                $link->method('query')
                    ->willReturnCallback(function () {
                        return new Delayed(100, $this->createMock(CommandResult::class));
                    });

                return new Success($link);
            });

        return $connector;
    }

    private function createPool(Connector $connector, int $maxConnections = 100, int $idleTimeout = 10): ConnectionPool
    {
        return $this->getMockBuilder(ConnectionPool::class)
            ->setConstructorArgs([
                $this->createMock(ConnectionConfig::class),
                $maxConnections,
                $idleTimeout,
                $connector
            ])
            ->getMockForAbstractClass();
    }

    public function testIdleConnectionsRemovedAfterTimeout(): \Generator
    {
        $connector = $this->createConnector();
        $pool = $this->createPool($connector, 10, 2);

        $count = 3;

        $promises = [];
        for ($i = 0; $i < $count; ++$i) {
            $promises[] = $pool->query("SELECT $i");
        }

        $this->assertCount($count, yield $promises);

        $this->assertSame($count, $pool->getConnectionCount());

        yield new Delayed(1000);

        $this->assertSame($count, $pool->getConnectionCount());

        yield new Delayed(1000);

        $this->assertSame(0, $pool->getConnectionCount());
    }

    public function testMaxConnectionCount(): \Generator
    {
        $connector = $this->createConnector();
        $pool = $this->createPool($connector, $maxConnections = 3);

        $count = 10;

        $promises = [];
        for ($i = 0; $i < $count; ++$i) {
            $promises[] = $pool->query("SELECT $i");
        }

        $this->assertSame($maxConnections, $pool->getConnectionCount());

        $expectedRuntime = 100 * \ceil($count / $maxConnections);

        $this->setMinimumRuntime($expectedRuntime);
        $this->setTimeout($expectedRuntime + 100);

        $this->assertCount($count, yield $promises);
    }
}
