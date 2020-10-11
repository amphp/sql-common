<?php

namespace Amp\Sql\Common\Test;

use Amp\PHPUnit\AsyncTestCase;
use Amp\Sql\Common\RetryConnector;
use Amp\Sql\ConnectionConfig;
use Amp\Sql\ConnectionException;
use Amp\Sql\Connector;
use Amp\Sql\Link;

class RetryConnectorTest extends AsyncTestCase
{
    public function testSuccessfulConnect()
    {
        $connector = $this->createMock(Connector::class);
        $connector->expects($this->once())
            ->method('connect')
            ->willReturn($this->createMock(Link::class));

        $retry = new RetryConnector($connector);

        $config = $this->getMockBuilder(ConnectionConfig::class)
            ->setConstructorArgs(['localhost', 5432])
            ->getMockForAbstractClass();

        $connection = $retry->connect($config);

        $this->assertInstanceOf(Link::class, $connection);
    }

    public function testFirstTryFailConnect()
    {
        $connector = $this->createMock(Connector::class);
        $connector->expects($this->exactly(2))
            ->method('connect')
            ->willReturnCallback(function (): Link {
                static $initial = true;

                if ($initial) {
                    $initial = false;
                    throw new ConnectionException;
                }

                return $this->createMock(Link::class);
            });

        $retry = new RetryConnector($connector);

        $config = $this->getMockBuilder(ConnectionConfig::class)
            ->setConstructorArgs(['localhost', 5432])
            ->getMockForAbstractClass();

        $connection = $retry->connect($config);

        $this->assertInstanceOf(Link::class, $connection);
    }

    public function testFailingConnect()
    {
        $tries = 3;

        $connector = $this->createMock(Connector::class);
        $connector->expects($this->exactly($tries))
            ->method('connect')
            ->willThrowException(new ConnectionException);

        $retry = new RetryConnector($connector, $tries);

        $config = $this->getMockBuilder(ConnectionConfig::class)
            ->setConstructorArgs(['localhost', 5432])
            ->getMockForAbstractClass();

        $this->expectException(ConnectionException::class);
        $this->expectExceptionMessage('Could not connect to database server');

        $connection = $retry->connect($config);
    }
}
