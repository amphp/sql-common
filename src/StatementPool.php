<?php

namespace Amp\Sql\Common;

use Amp\Sql\Pool;
use Amp\Sql\Result;
use Amp\Sql\Statement;
use Revolt\EventLoop;

class StatementPool implements Statement
{
    private readonly Pool $pool;

    private readonly \SplQueue $statements;

    private readonly string $sql;

    private int $lastUsedAt;

    private readonly string $timeoutWatcher;

    /** @var \Closure(string):Statement */
    private readonly \Closure $prepare;

    /**
     * @param Pool $pool Pool used to prepare statements for execution.
     * @param string $sql SQL statement to prepare
     * @param \Closure(string):Statement $prepare Callable that returns a new prepared statement.
     */
    public function __construct(Pool $pool, string $sql, \Closure $prepare)
    {
        $this->lastUsedAt = \time();
        $this->statements = $statements = new \SplQueue;
        $this->pool = $pool;
        $this->prepare = $prepare;
        $this->sql = $sql;

        $this->timeoutWatcher = EventLoop::repeat(1, static function () use ($pool, $statements): void {
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

        EventLoop::unreference($this->timeoutWatcher);
    }

    public function __destruct()
    {
        EventLoop::cancel($this->timeoutWatcher);
    }

    /**
     * @param \Closure():void $release
     */
    protected function createResult(Result $result, \Closure $release): Result
    {
        return new PooledResult($result, $release);
    }

    /**
     * Unlike regular statements, as long as the pool is open this statement will not die.
     */
    public function execute(array $params = []): Result
    {
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

    protected function pop(): Statement
    {
        while (!$this->statements->isEmpty()) {
            $statement = $this->statements->dequeue();

            if ($statement->isAlive()) {
                return $statement;
            }
        }

        return ($this->prepare)($this->sql);
    }

    public function isAlive(): bool
    {
        return $this->pool->isAlive();
    }

    public function getQuery(): string
    {
        return $this->sql;
    }

    public function getLastUsedAt(): int
    {
        return $this->lastUsedAt;
    }
}
