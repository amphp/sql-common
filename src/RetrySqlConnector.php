<?php

namespace Amp\Sql\Common;

use Amp\Cancellation;
use Amp\CompositeException;
use Amp\Sql\ConnectionException;
use Amp\Sql\Link;
use Amp\Sql\SqlConfig;
use Amp\Sql\SqlConnector;

/**
 * @template TConfig extends SqlConfig
 * @template TLink extends Link
 * @implements SqlConnector<TConfig, TLink>
 */
final class RetrySqlConnector implements SqlConnector
{
    /**
     * @param SqlConnector<TConfig, TLink> $connector
     */
    public function __construct(
        private readonly SqlConnector $connector,
        private readonly int $maxTries = 3,
    ) {
        if ($maxTries <= 0) {
            throw new \Error('The number of tries must be 1 or greater');
        }
    }

    public function connect(SqlConfig $config, ?Cancellation $cancellation = null): Link
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
