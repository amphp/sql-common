<?php

namespace Amp\Sql\Common;

use Amp\DeferredFuture;
use Amp\Sql\Pool;
use Amp\Sql\Result;
use Amp\Sql\SqlException;
use Amp\Sql\Statement;
use Revolt\EventLoop;

/**
 * @template TResult of Result
 * @template TStatement of Statement
 * @implements Statement<TResult>
 */
abstract class StatementPool implements Statement
{
    private readonly Pool $pool;

    /** @var \SplQueue<TStatement> */
    private readonly \SplQueue $statements;

    private readonly string $sql;

    private int $lastUsedAt;

    /** @var \Closure(string):TStatement */
    private readonly \Closure $prepare;

    private readonly DeferredFuture $onClose;

    /**
     * @param TResult $result
     * @param \Closure():void $release
     */
    abstract protected function createResult(Result $result, \Closure $release): Result;

    /**
     * @param Pool $pool Pool used to prepare statements for execution.
     * @param string $sql SQL statement to prepare
     * @param \Closure(string):TStatement $prepare Callable that returns a new prepared statement.
     */
    public function __construct(Pool $pool, string $sql, \Closure $prepare)
    {
        $this->lastUsedAt = \time();
        $this->statements = $statements = new \SplQueue;
        $this->pool = $pool;
        $this->prepare = $prepare;
        $this->sql = $sql;
        $this->onClose = $onClose = new DeferredFuture();

        $timeoutWatcher = EventLoop::repeat(1, static function () use ($pool, $statements): void {
            $now = \time();
            $idleTimeout = ((int) ($pool->getIdleTimeout() / 10)) ?: 1;

            while (!$statements->isEmpty()) {
                $statement = $statements->bottom();
                \assert($statement instanceof Statement);

                if ($statement->getLastUsedAt() + $idleTimeout > $now) {
                    return;
                }

                $statements->shift();
            }
        });

        EventLoop::unreference($timeoutWatcher);
        $this->onClose(static fn () => EventLoop::cancel($timeoutWatcher));

        $this->pool->onClose(static fn () => $onClose->isComplete() || $onClose->complete());
    }

    public function __destruct()
    {
        $this->close();
    }

    /**
     * Unlike regular statements, as long as the pool is open this statement will not die.
     *
     * @return TResult
     */
    public function execute(array $params = []): Result
    {
        if ($this->isClosed()) {
            throw new SqlException('The statement has been closed or the connection pool has been closed');
        }

        $this->lastUsedAt = \time();

        $statement = $this->pop();

        try {
            $result = $statement->execute($params);
        } catch (\Throwable $exception) {
            $this->push($statement);
            throw $exception;
        }

        return $this->createResult($result, fn () => $this->push($statement));
    }

    /**
     * Only retains statements if less than 10% of the pool is consumed by this statement and the pool has
     * available connections.
     *
     * @param TStatement $statement
     */
    protected function push(Statement $statement): void
    {
        $maxConnections = $this->pool->getConnectionLimit();

        if ($this->statements->count() > ($maxConnections / 10)) {
            return;
        }

        if ($maxConnections === $this->pool->getConnectionCount() && $this->pool->getIdleConnectionCount() === 0) {
            return;
        }

        $this->statements->enqueue($statement);
    }

    /**
     * @return TStatement
     */
    protected function pop(): Statement
    {
        while (!$this->statements->isEmpty()) {
            $statement = $this->statements->dequeue();
            \assert($statement instanceof Statement);

            if (!$statement->isClosed()) {
                return $statement;
            }
        }

        return ($this->prepare)($this->sql);
    }

    final public function close(): void
    {
        if (!$this->onClose->isComplete()) {
            $this->onClose->complete();
        }
    }

    final public function isClosed(): bool
    {
        return $this->onClose->isComplete();
    }

    final public function onClose(\Closure $onClose): void
    {
        $this->onClose->getFuture()->finally($onClose);
    }

    final public function getQuery(): string
    {
        return $this->sql;
    }

    final public function getLastUsedAt(): int
    {
        return $this->lastUsedAt;
    }
}
