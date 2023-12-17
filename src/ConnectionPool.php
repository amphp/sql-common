<?php declare(strict_types=1);

namespace Amp\Sql\Common;

use Amp\DeferredFuture;
use Amp\ForbidCloning;
use Amp\ForbidSerialization;
use Amp\Future;
use Amp\Sql\Connection;
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

/**
 * @template TConfig of SqlConfig
 * @template TResult of Result
 * @template TStatement of Statement<TResult>
 * @template TTransaction of Transaction
 * @template TConnection of Connection<TConfig, TResult, TStatement, TTransaction>
 *
 * @implements Pool<TConfig, TResult, TStatement, TTransaction>
 */
abstract class ConnectionPool implements Pool
{
    use ForbidCloning;
    use ForbidSerialization;

    public const DEFAULT_MAX_CONNECTIONS = 100;
    public const DEFAULT_IDLE_TIMEOUT = 60;

    /** @var \SplQueue<TConnection> */
    private readonly \SplQueue $idle;

    /** @var \SplObjectStorage<TConnection, null> */
    private readonly \SplObjectStorage $connections;

    /** @var Future<TConnection>|null */
    private ?Future $future = null;

    /** @var DeferredFuture<TConnection>|null */
    private ?DeferredFuture $awaitingConnection = null;

    private readonly DeferredFuture $onClose;

    /**
     * Creates a Statement of the appropriate type using the Statement object returned by the Link object and the
     * given release callable.
     *
     * @param TStatement $statement
     * @param \Closure():void $release
     *
     * @return TStatement
     */
    abstract protected function createStatement(Statement $statement, \Closure $release): Statement;

    /**
     * Creates a Result of the appropriate type using the Result object returned by the Link object and the
     * given release callable.
     *
     * @param TResult $result
     * @param \Closure():void $release
     * @return TResult
     */
    abstract protected function createResult(Result $result, \Closure $release): Result;

    /**
     * @param \Closure(string):TStatement $prepare
     *
     * @return TStatement
     */
    abstract protected function createStatementPool(string $sql, \Closure $prepare): Statement;

    /**
     * Creates a Transaction of the appropriate type using the Transaction object returned by the Link object and the
     * given release callable.
     *
     * @param TTransaction $transaction
     * @param \Closure():void $release
     *
     * @return TTransaction
     */
    abstract protected function createTransaction(Transaction $transaction, \Closure $release): Transaction;

    /**
     * @param TConfig $config
     * @param SqlConnector<TConfig, TConnection> $connector
     * @param positive-int $maxConnections Maximum number of active connections in the pool.
     * @param positive-int $idleTimeout Number of seconds until idle connections are removed from the pool.
     */
    public function __construct(
        private readonly SqlConfig $config,
        private readonly SqlConnector $connector,
        private readonly int $maxConnections = self::DEFAULT_MAX_CONNECTIONS,
        private int $idleTimeout = self::DEFAULT_IDLE_TIMEOUT,
        private TransactionIsolation $transactionIsolation = TransactionIsolationLevel::Committed,
    ) {
        /** @psalm-suppress TypeDoesNotContainType */
        if ($this->idleTimeout < 1) {
            throw new \Error("The idle timeout must be 1 or greater");
        }

        /** @psalm-suppress TypeDoesNotContainType */
        if ($this->maxConnections < 1) {
            throw new \Error("Pool must contain at least one connection");
        }

        $this->connections = $connections = new \SplObjectStorage();
        $this->idle = $idle = new \SplQueue();
        $this->onClose = new DeferredFuture();

        $idleTimeout = &$this->idleTimeout;

        $timeoutWatcher = EventLoop::repeat(1, static function () use (&$idleTimeout, $connections, $idle) {
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

        EventLoop::unreference($timeoutWatcher);
        $this->onClose(static fn () => EventLoop::cancel($timeoutWatcher));
    }

    public function __destruct()
    {
        $this->close();
    }

    public function getTransactionIsolation(): TransactionIsolation
    {
        return $this->transactionIsolation;
    }

    public function setTransactionIsolation(TransactionIsolation $isolation): void
    {
        $this->transactionIsolation = $isolation;
    }

    public function getConfig(): SqlConfig
    {
        return $this->config;
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

    public function isClosed(): bool
    {
        return $this->onClose->isComplete();
    }

    public function onClose(\Closure $onClose): void
    {
        $this->onClose->getFuture()->finally($onClose);
    }

    /**
     * Close all connections in the pool. No further queries may be made after a pool is closed.
     */
    public function close(): void
    {
        if ($this->onClose->isComplete()) {
            return;
        }

        foreach ($this->connections as $connection) {
            // Avoid first class callable syntax to avoid psalm crash
            /** @psalm-suppress MissingClosureReturnType */
            async(fn () => $connection->close())->ignore();
        }

        $this->onClose->complete();

        $this->awaitingConnection?->error(new SqlException("Connection pool closed"));
        $this->awaitingConnection = null;
    }

    /**
     * @return TConnection
     *
     * @throws SqlException
     */
    public function extractConnection(): Connection
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
     * @return TConnection
     *
     * @throws SqlException If creating a new connection fails.
     * @throws \Error If the pool has been closed.
     */
    protected function pop(): Connection
    {
        if ($this->isClosed()) {
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

                    if ($this->isClosed()) {
                        $connection->close();
                        break 2; // Break to throwing exception.
                    }

                    $this->connections->attach($connection);
                    return $connection;
                }

                // All possible connections busy, so wait until one becomes available.
                try {
                    $this->awaitingConnection = new DeferredFuture;
                    // Connection will be pulled from $this->idle when future is resolved.
                    ($this->future = $this->awaitingConnection->getFuture())->await();
                } finally {
                    $this->awaitingConnection = null;
                    $this->future = null;
                }
            }

            $connection = $this->idle->dequeue();
            \assert($connection instanceof Link);

            if (!$connection->isClosed()) {
                return $connection;
            }

            $this->connections->detach($connection);
        } while (!$this->isClosed());

        throw new SqlException("Pool closed before an active connection could be obtained");
    }

    /**
     * @param TConnection $connection
     *
     * @throws \Error If the connection is not part of this pool.
     */
    protected function push(Connection $connection): void
    {
        \assert(isset($this->connections[$connection]), 'Connection is not part of this pool');

        if ($connection->isClosed()) {
            $this->connections->detach($connection);
        } else {
            $this->idle->enqueue($connection);
        }

        $this->awaitingConnection?->complete($connection);
        $this->awaitingConnection = null;
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
        /** @psalm-suppress InvalidArgument Psalm is not properly detecting the templated return type. */
        return $this->createStatementPool($sql, $this->prepareStatement(...));
    }

    /**
     * Prepares a new statement on an available connection.
     *
     * @return TStatement
     *
     * @throws SqlException
     */
    private function prepareStatement(string $sql): Statement
    {
        $connection = $this->pop();

        try {
            $statement = $connection->prepare($sql);
        } catch (\Throwable $exception) {
            $this->push($connection);
            throw $exception;
        }

        return $this->createStatement($statement, fn () => $this->push($connection));
    }

    public function beginTransaction(): Transaction
    {
        $connection = $this->pop();

        try {
            $connection->setTransactionIsolation($this->transactionIsolation);
            $transaction = $connection->beginTransaction();
        } catch (\Throwable $exception) {
            $this->push($connection);
            throw $exception;
        }

        return $this->createTransaction($transaction, fn () => $this->push($connection));
    }
}
