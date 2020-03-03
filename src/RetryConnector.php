<?php

namespace Amp\Sql\Common;

use Amp\MultiReasonException;
use Amp\Promise;
use Amp\Sql\ConnectionConfig;
use Amp\Sql\ConnectionException;
use Amp\Sql\Connector;
use function Amp\call;

final class RetryConnector implements Connector
{
    /** @var Connector */
    private $connector;

    /** @var int */
    private $maxTries;

    public function __construct(Connector $connector, int $maxTries = 3)
    {
        if ($maxTries <= 0) {
            throw new \Error('The number of tries must be 1 or greater');
        }

        $this->connector = $connector;
        $this->maxTries = $maxTries;
    }

    public function connect(ConnectionConfig $config): Promise
    {
        return call(function () use ($config) {
            $tries = 0;
            $exceptions = [];

            do {
                try {
                    return yield $this->connector->connect($config);
                } catch (\Exception $exception) {
                    $exceptions[] = $exception;
                }
            } while (++$tries < $this->maxTries);

            $name = $config->getHost() . ':' . $config->getPort();

            throw new ConnectionException(
                "Could not connect to database server at {$name} after {$tries} tries",
                0,
                new MultiReasonException($exceptions)
            );
        });
    }
}
