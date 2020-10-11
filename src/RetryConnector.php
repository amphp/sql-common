<?php

namespace Amp\Sql\Common;

use Amp\MultiReasonException;
use Amp\Sql\ConnectionConfig;
use Amp\Sql\ConnectionException;
use Amp\Sql\Connector;
use Amp\Sql\Link;

final class RetryConnector implements Connector
{
    private Connector $connector;

    private int $maxTries;

    public function __construct(Connector $connector, int $maxTries = 3)
    {
        if ($maxTries <= 0) {
            throw new \Error('The number of tries must be 1 or greater');
        }

        $this->connector = $connector;
        $this->maxTries = $maxTries;
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
            new MultiReasonException($exceptions)
        );
    }
}
