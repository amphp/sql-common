<?php

namespace Amp\Sql\Common;

use Amp\CompositeException;
use Amp\Sql\ConnectionConfig;
use Amp\Sql\ConnectionException;
use Amp\Sql\Connector;
use Amp\Sql\Link;

final class RetryConnector implements Connector
{
    public function __construct(
        private readonly Connector $connector,
        private readonly int $maxTries = 3,
    ) {
        if ($maxTries <= 0) {
            throw new \Error('The number of tries must be 1 or greater');
        }
    }

    public function connect(ConnectionConfig $config): Link
    {
        $tries = 0;
        $exceptions = [];

        do {
            try {
                return $this->connector->connect($config);
            } catch (\Exception $exception) {
                $exceptions[] = $exception;
            }
        } while (++$tries < $this->maxTries);

        $name = $config->getHost() . ':' . $config->getPort();

        throw new ConnectionException(
            "Could not connect to database server at {$name} after {$tries} tries",
            0,
            new CompositeException($exceptions)
        );
    }
}
