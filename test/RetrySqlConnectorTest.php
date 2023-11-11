<?php declare(strict_types=1);

namespace Amp\Sql\Common\Test;

use Amp\PHPUnit\AsyncTestCase;
use Amp\Sql\Common\RetrySqlConnector;
use Amp\Sql\Connection;
use Amp\Sql\ConnectionException;
use Amp\Sql\SqlConfig;
use Amp\Sql\SqlConnector;

class RetrySqlConnectorTest extends AsyncTestCase
{
    public function testSuccessfulConnect()
    {
        $connector = $this->createMock(SqlConnector::class);
        $connector->expects($this->once())
            ->method('connect')
            ->willReturn($this->createMock(Connection::class));

        $retry = new RetrySqlConnector($connector);

        $config = $this->getMockBuilder(SqlConfig::class)
            ->setConstructorArgs(['localhost', 5432])
            ->getMockForAbstractClass();

        $connection = $retry->connect($config);

        $this->assertInstanceOf(Connection::class, $connection);
    }

    public function testFirstTryFailConnect()
    {
        $connector = $this->createMock(SqlConnector::class);
        $connector->expects($this->exactly(2))
            ->method('connect')
            ->willReturnCallback(function (): Connection {
                static $initial = true;

                if ($initial) {
                    $initial = false;
                    throw new ConnectionException;
                }

                return $this->createMock(Connection::class);
            });

        $retry = new RetrySqlConnector($connector);

        $config = $this->getMockBuilder(SqlConfig::class)
            ->setConstructorArgs(['localhost', 5432])
            ->getMockForAbstractClass();

        $connection = $retry->connect($config);

        $this->assertInstanceOf(Connection::class, $connection);
    }

    public function testFailingConnect()
    {
        $tries = 3;

        $connector = $this->createMock(SqlConnector::class);
        $connector->expects($this->exactly($tries))
            ->method('connect')
            ->willThrowException(new ConnectionException);

        $retry = new RetrySqlConnector($connector, $tries);

        $config = $this->getMockBuilder(SqlConfig::class)
            ->setConstructorArgs(['localhost', 5432])
            ->getMockForAbstractClass();

        $this->expectException(ConnectionException::class);
        $this->expectExceptionMessage('Could not connect to database server');

        $connection = $retry->connect($config);
    }
}
