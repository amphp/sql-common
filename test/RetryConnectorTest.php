<?php

namespace Amp\Sql\Common\Test;

use Amp\Failure;
use Amp\PHPUnit\AsyncTestCase;
use Amp\Sql\Common\RetryConnector;
use Amp\Sql\ConnectionConfig;
use Amp\Sql\ConnectionException;
use Amp\Sql\Connector;
use Amp\Sql\Link;
use Amp\Success;

class RetryConnectorTest extends AsyncTestCase
{
    public function testSuccessfulConnect(): \Generator
    {
        $connector = $this->createMock(Connector::class);
        $connector->expects($this->once())
            ->method('connect')
            ->willReturn(new Success($this->createMock(Link::class)));

        $retry = new RetryConnector($connector);

        $config = $this->getMockBuilder(ConnectionConfig::class)
            ->setConstructorArgs(['localhost', 5432])
            ->getMockForAbstractClass();

        $connection = yield $retry->connect($config);

        $this->assertInstanceOf(Link::class, $connection);
    }

    public function testFirstTryFailConnect(): \Generator
    {
        $connector = $this->createMock(Connector::class);
        $connector->expects($this->exactly(2))
            ->method('connect')
            ->willReturnOnConsecutiveCalls(
                new Failure(new ConnectionException),
                new Success($this->createMock(Link::class))
            );

        $retry = new RetryConnector($connector);

        $config = $this->getMockBuilder(ConnectionConfig::class)
            ->setConstructorArgs(['localhost', 5432])
            ->getMockForAbstractClass();

        $connection = yield $retry->connect($config);

        $this->assertInstanceOf(Link::class, $connection);
    }

    public function testFailingConnect(): \Generator
    {
        $tries = 3;

        $connector = $this->createMock(Connector::class);
        $connector->expects($this->exactly($tries))
            ->method('connect')
            ->willReturn(new Failure(new ConnectionException));

        $retry = new RetryConnector($connector, $tries);

        $config = $this->getMockBuilder(ConnectionConfig::class)
            ->setConstructorArgs(['localhost', 5432])
            ->getMockForAbstractClass();

        $this->expectException(ConnectionException::class);
        $this->expectExceptionMessage('Could not connect to database server');

        $connection = yield $retry->connect($config);
    }
}
