<?php

namespace Amp\Sql\Common\Test;

use Amp\Future;
use Amp\PHPUnit\AsyncTestCase;
use Amp\Sql\Common\ConnectionPool;
use Amp\Sql\ConnectionConfig;
use Amp\Sql\Connector;
use Amp\Sql\Link;
use Amp\Sql\Result;
use function Amp\delay;
use function Amp\async;

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
            ->willReturnCallback(function () use ($now): Link {
                $link = $this->createMock(Link::class);
                $link->method('getLastUsedAt')
                    ->willReturn($now);

                $link->method('isAlive')
                    ->willReturn(true);

                $link->method('query')
                    ->willReturnCallback(function () {
                        delay(0.1);
                        return $this->createMock(Result::class);
                    });

                return $link;
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

    public function testIdleConnectionsRemovedAfterTimeout()
    {
        $connector = $this->createConnector();
        $pool = $this->createPool($connector, 10, 2);

        $count = 3;

        $futures = [];
        for ($i = 0; $i < $count; ++$i) {
            $futures[] = async(fn() => $pool->query("SELECT $i"));
        }

        $this->assertCount($count, Future\all($futures));

        unset($futures); // Remove references to results so they are destructed.

        $this->assertSame($count, $pool->getConnectionCount());

        delay(1);

        $this->assertSame($count, $pool->getConnectionCount());

        delay(1);

        $this->assertSame(0, $pool->getConnectionCount());
    }

    public function testMaxConnectionCount()
    {
        $connector = $this->createConnector();
        $pool = $this->createPool($connector, $maxConnections = 3);

        $count = 10;

        $futures = [];
        for ($i = 0; $i < $count; ++$i) {
            $futures[] = async(fn() => $pool->query("SELECT $i"));
        }

        $expectedRuntime = 0.1 * \ceil($count / $maxConnections);

        $this->setMinimumRuntime($expectedRuntime);
        $this->setTimeout($expectedRuntime + 0.1);

        foreach ($futures as $future) {
            /** @var Result $result */
            $result = $future->await();
            \iterator_to_array($result);
        }

        $this->assertSame($maxConnections, $pool->getConnectionCount());
    }
}
