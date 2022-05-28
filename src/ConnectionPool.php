<?php

namespace Amp\Sql\Common;

use Amp\DeferredFuture;
use Amp\Future;
use Amp\Sql\Link;
use Amp\Sql\Pool;
use Amp\Sql\Result;
use Amp\Sql\SqlConfig;
use Amp\Sql\SqlConnector;
use Amp\Sql\SqlException;
use Amp\Sql\Statement;
use Amp\Sql\Transaction;
use Amp\Sql\TransactionIsolation;
use Amp\Sql\TransactionIsolationLevel;
use Revolt\EventLoop;
use function Amp\async;

abstract class ConnectionPool implements Pool
{
    public const DEFAULT_MAX_CONNECTIONS = 100;
    public const DEFAULT_IDLE_TIMEOUT = 60;

    private readonly \SplQueue $idle;

    private readonly \SplObjectStorage $connections;

    /** @var Future<Link>|null */
    private ?Future $future = null;

    private ?DeferredFuture $deferred = null;

    private readonly string $timeoutWatcher;

    private bool $closed = false;

    /**
     * Creates a Statement of the appropriate type using the Statement object returned by the Link object and the
     * given release callable.
     *
     * @param \Closure():void $release
     */
    abstract protected function createStatement(Statement $statement, \Closure $release): Statement;

    /**
     * @param \Closure(string):Statement $prepare
     */
    abstract protected function createStatementPool(Pool $pool, string $sql, \Closure $prepare): StatementPool;

    /**
     * Creates a Transaction of the appropriate type using the Transaction object returned by the Link object and the
     * given release callable.
     *
     * @param \Closure():void $release
     */
    abstract protected function createTransaction(Transaction $transaction, \Closure $release): Transaction;

    /**
     * @param positive-int $maxConnections Maximum number of active connections in the pool.
     * @param positive-int $idleTimeout Number of seconds until idle connections are removed from the pool.
     */
    public function __construct(
        private readonly SqlConfig $config,
        private readonly SqlConnector $connector,
        private readonly int $maxConnections = self::DEFAULT_MAX_CONNECTIONS,
        private int $idleTimeout = self::DEFAULT_IDLE_TIMEOUT,
    ) {
        /** @psalm-suppress TypeDoesNotContainType */
        if ($this->idleTimeout < 1) {
            throw new \Error("The idle timeout must be 1 or greater");
        }

        /** @psalm-suppress TypeDoesNotContainType */
        if ($this->maxConnections < 1) {
            throw new \Error("Pool must contain at least one connection");
        }

        $this->connections = $connections = new \SplObjectStorage;
        $this->idle = $idle = new \SplQueue;

        $idleTimeout = &$this->idleTimeout;

        $this->timeoutWatcher = EventLoop::repeat(1, static function () use (&$idleTimeout, $connections, $idle) {
            $now = \time();
            while (!$idle->isEmpty()) {
                $connection = $idle->bottom();
                \assert($connection instanceof Link);

                if ($connection->getLastUsedAt() + $idleTimeout > $now) {
                    return;
                }

                // Close connection and remove it from the pool.
                $idle->shift();
                /** @psalm-suppress InvalidArgument SplObjectStorage::detach() expects an argument. */
                $connections->detach($connection);
                $connection->close();
            }
        });

        EventLoop::unreference($this->timeoutWatcher);
    }

    public function __destruct()
    {
        EventLoop::cancel($this->timeoutWatcher);
    }

    /**
     * Creates a ResultSet of the appropriate type using the ResultSet object returned by the Link object and the
     * given release callable.
     *
     * @param \Closure():void $release
     */
    protected function createResult(Result $result, \Closure $release): Result
    {
        return new PooledResult($result, $release);
    }

    public function getIdleTimeout(): int
    {
        return $this->idleTimeout;
    }

    public function getLastUsedAt(): int
    {
        // Simple implementation... can be improved if needed.

        $time = 0;

        foreach ($this->connections as $connection) {
            \assert($connection instanceof Link);
            if (($lastUsedAt = $connection->getLastUsedAt()) > $time) {
                $time = $lastUsedAt;
            }
        }

        return $time;
    }

    public function isAlive(): bool
    {
        return !$this->closed;
    }

    /**
     * Close all connections in the pool. No further queries may be made after a pool is closed.
     */
    public function close(): void
    {
        $this->closed = true;
        foreach ($this->connections as $connection) {
            $connection->close();
        }

        while (!$this->idle->isEmpty()) {
            $this->idle->dequeue();
        }

        if ($this->deferred instanceof DeferredFuture) {
            $deferred = $this->deferred;
            $this->deferred = null;
            $deferred->error(new SqlException("Connection pool closed"));
        }
    }

    public function extractConnection(): Link
    {
        $connection = $this->pop();
        $this->connections->detach($connection);
        return $connection;
    }

    public function getConnectionCount(): int
    {
        return $this->connections->count();
    }

    public function getIdleConnectionCount(): int
    {
        return $this->idle->count();
    }

    public function getConnectionLimit(): int
    {
        return $this->maxConnections;
    }

    /**
     * @throws SqlException If creating a new connection fails.
     * @throws \Error If the pool has been closed.
     */
    protected function pop(): Link
    {
        if ($this->closed) {
            throw new \Error("The pool has been closed");
        }

        while ($this->future !== null) {
            $this->future->await(); // Prevent simultaneous connection creation or waiting.
        }

        do {
            // While loop to ensure an idle connection is available after futures below are resolved.
            while ($this->idle->isEmpty()) {
                if ($this->connections->count() < $this->getConnectionLimit()) {
                    // Max connection count has not been reached, so open another connection.
                    try {
                        $connection = (
                            $this->future = async(fn () => $this->connector->connect($this->config))
                        )->await();
                        /** @psalm-suppress DocblockTypeContradiction */
                        if (!$connection instanceof Link) {
                            throw new \Error(\sprintf(
                                "%s::connect() must resolve to an instance of %s",
                                \get_class($this->connector),
                                Link::class
                            ));
                        }
                    } finally {
                        $this->future = null;
                    }

                    $this->connections->attach($connection);
                    return $connection;
                }

                // All possible connections busy, so wait until one becomes available.
                try {
                    $this->deferred = new DeferredFuture;
                    // Connection will be pulled from $this->idle when future is resolved.
                    ($this->future = $this->deferred->getFuture())->await();
                } finally {
                    $this->deferred = null;
                    $this->future = null;
                }
            }

            $connection = $this->idle->dequeue();
            \assert($connection instanceof Link);

            if ($connection->isAlive()) {
                return $connection;
            }

            $this->connections->detach($connection);
        } while (!$this->closed);

        throw new SqlException("Pool closed before an active connection could be obtained");
    }

    /**
     * @throws \Error If the connection is not part of this pool.
     */
    protected function push(Link $connection): void
    {
        \assert(isset($this->connections[$connection]), 'Connection is not part of this pool');

        if ($connection->isAlive()) {
            $this->idle->enqueue($connection);
        } else {
            $this->connections->detach($connection);
        }

        $this->deferred?->complete($connection);
    }

    public function query(string $sql): Result
    {
        $connection = $this->pop();

        try {
            $result = $connection->query($sql);
        } catch (\Throwable $exception) {
            $this->push($connection);
            throw $exception;
        }

        return $this->createResult($result, fn () => $this->push($connection));
    }

    public function execute(string $sql, array $params = []): Result
    {
        $connection = $this->pop();

        try {
            $result = $connection->execute($sql, $params);
        } catch (\Throwable $exception) {
            $this->push($connection);
            throw $exception;
        }

        return $this->createResult($result, fn () => $this->push($connection));
    }

    /**
     * Prepared statements returned by this method will stay alive as long as the pool remains open.
     */
    public function prepare(string $sql): Statement
    {
        return $this->createStatementPool($this, $sql, $this->prepareStatement(...));
    }

    /**
     * Prepares a new statement on an available connection.
     *
     *
     *
     * @throws SqlException
     */
    private function prepareStatement(string $sql): Statement
    {
        $connection = $this->pop();

        try {
            $statement = $connection->prepare($sql);
            \assert($statement instanceof Statement);
        } catch (\Throwable $exception) {
            $this->push($connection);
            throw $exception;
        }

        return $this->createStatement($statement, fn () => $this->push($connection));
    }

    public function beginTransaction(
        TransactionIsolation $isolation = TransactionIsolationLevel::Committed
    ): Transaction {
        $connection = $this->pop();

        try {
            $transaction = $connection->beginTransaction($isolation);
        } catch (\Throwable $exception) {
            $this->push($connection);
            throw $exception;
        }

        return $this->createTransaction($transaction, fn () => $this->push($connection));
    }
}
