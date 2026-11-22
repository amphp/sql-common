<?php declare(strict_types=1);

namespace Amp\Sql\Common\Test;

use Amp\DeferredFuture;
use Amp\Future;
use Amp\PHPUnit\AsyncTestCase;
use Amp\Sql\Common\SqlCommonConnectionPool;
use Amp\Sql\Common\Test\Stub\StubSqlPooledResult;
use Amp\Sql\SqlConfig;
use Amp\Sql\SqlConnection;
use Amp\Sql\SqlConnector;
use Amp\Sql\SqlException;
use Amp\Sql\SqlResult;
use function Amp\async;
use function Amp\delay;

class SqlCommonConnectionPoolTest extends AsyncTestCase
{
    public function testInvalidMaxConnections()
    {
        $this->expectException(\Error::class);
        $this->expectExceptionMessage('Pool must contain at least one connection');

        $this->getMockBuilder(SqlCommonConnectionPool::class)
            ->setConstructorArgs([
                $this->createMock(SqlConfig::class),
                $this->createMock(SqlConnector::class),
                0,
            ])
            ->getMock();
    }

    private function createConnector(): SqlConnector
    {
        $now = \time();

        $connector = $this->createMock(SqlConnector::class);
        $connector->method('connect')
            ->willReturnCallback(function () use ($now): SqlConnection {
                $connection = $this->createMock(SqlConnection::class);
                $connection->method('getLastUsedAt')
                    ->willReturn($now);

                $connection->method('isClosed')
                    ->willReturn(false);

                $connection->method('query')
                    ->willReturnCallback(function () {
                        delay(0.1);
                        return $this->createMock(SqlResult::class);
                    });

                return $connection;
            });

        return $connector;
    }

    private function createPool(SqlConnector $connector, int $maxConnections = 100, int $idleTimeout = 10): SqlCommonConnectionPool
    {
        $pool = $this->getMockBuilder(SqlCommonConnectionPool::class)
            ->setConstructorArgs([
                $this->createMock(SqlConfig::class),
                $connector,
                $maxConnections,
                $idleTimeout,
            ])
            ->getMockForAbstractClass();

        $pool->method('createResult')
            ->willReturnCallback(fn (SqlResult $result, \Closure $release) => new StubSqlPooledResult($result, $release));

        return $pool;
    }

    public function testIdleConnectionsRemovedAfterTimeout()
    {
        $connector = $this->createConnector();
        $pool = $this->createPool($connector, 10, 2);

        $count = 3;

        $futures = [];
        for ($i = 0; $i < $count; ++$i) {
            $futures[] = async(fn () => $pool->query("SELECT $i"));
        }

        self::assertCount($count, Future\await($futures));

        unset($futures); // Remove references to results so they are destructed.

        self::assertSame($count, $pool->getConnectionCount());

        delay(1);

        self::assertSame($count, $pool->getConnectionCount());

        delay(1);

        self::assertSame(0, $pool->getConnectionCount());
    }

    public function testMaxConnectionCount()
    {
        $connector = $this->createConnector();
        $pool = $this->createPool($connector, $maxConnections = 3);

        $count = 10;

        $futures = [];
        for ($i = 0; $i < $count; ++$i) {
            $futures[] = async(fn () => \iterator_to_array($pool->query("SELECT $i")));
        }

        $expectedRuntime = 0.1 * \ceil($count / $maxConnections);

        $this->setMinimumRuntime($expectedRuntime);
        $this->setTimeout($expectedRuntime + 1);

        Future\await($futures);

        self::assertSame($maxConnections, $pool->getConnectionCount());
    }

    /**
     * @param \Closure():void $onConnectionClose Invoked from each connection's close().
     */
    private function createSlowClosingConnector(\Closure $onConnectionClose, float $connectDelay = 0): SqlConnector
    {
        $connector = $this->createMock(SqlConnector::class);
        $connector->method('connect')
            ->willReturnCallback(function () use ($onConnectionClose, $connectDelay): SqlConnection {
                delay($connectDelay);

                $closed = false;
                $connection = $this->createMock(SqlConnection::class);
                $connection->method('getLastUsedAt')->willReturn(\time());
                $connection->method('isClosed')->willReturnCallback(function () use (&$closed) {
                    return $closed;
                });
                $connection->method('query')->willReturnCallback(fn () => $this->createMock(SqlResult::class));
                $connection->method('close')->willReturnCallback(function () use (&$closed, $onConnectionClose) {
                    if ($closed) {
                        return;
                    }

                    $closed = true;
                    delay(0.1); // Closing takes time, e.g. waiting for a child process to exit.
                    $onConnectionClose();
                });

                return $connection;
            });

        return $connector;
    }

    private function awaitOnClose(SqlCommonConnectionPool $pool): void
    {
        $deferred = new DeferredFuture();
        $pool->onClose($deferred->complete(...));
        $deferred->getFuture()->await();
    }

    public function testOnCloseInvokedAfterConnectionsClosed()
    {
        $closedConnections = 0;
        $pool = $this->createPool($this->createSlowClosingConnector(function () use (&$closedConnections) {
            ++$closedConnections;
        }));

        Future\await([
            async(fn () => $pool->query("SELECT 1")),
            async(fn () => $pool->query("SELECT 2")),
        ]);

        self::assertSame(2, $pool->getConnectionCount());

        $pool->close();

        self::assertTrue($pool->isClosed());
        self::assertSame(0, $closedConnections);

        $this->awaitOnClose($pool);

        self::assertSame(2, $closedConnections);
    }

    public function testOnCloseInvokedWhenConnectionCloseFails()
    {
        $pool = $this->createPool($this->createSlowClosingConnector(function () {
            throw new SqlException("Close failed");
        }));

        $pool->query("SELECT 1");

        $pool->close();

        $this->setTimeout(1);
        $this->awaitOnClose($pool);

        self::assertTrue($pool->isClosed());
    }

    public function testOnCloseAwaitsConnectionOpenedDuringClose()
    {
        $closedConnections = 0;
        $pool = $this->createPool($this->createSlowClosingConnector(function () use (&$closedConnections) {
            ++$closedConnections;
        }, 0.1));

        $query = async(fn () => $pool->query("SELECT 1"));

        delay(0.05); // Connection attempt is pending.

        $pool->close();

        $this->awaitOnClose($pool);

        self::assertSame(1, $closedConnections);

        $this->expectException(SqlException::class);
        $query->await();
    }

    public function testCloseIsIdempotent()
    {
        $closedConnections = 0;
        $pool = $this->createPool($this->createSlowClosingConnector(function () use (&$closedConnections) {
            ++$closedConnections;
        }));

        $pool->query("SELECT 1");

        $pool->close();
        $pool->close();

        $this->awaitOnClose($pool);
        $pool->close();

        self::assertSame(1, $closedConnections);
    }
}
