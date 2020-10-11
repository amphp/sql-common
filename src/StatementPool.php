<?php

namespace Amp\Sql\Common;

use Amp\Loop;
use Amp\Sql\Pool;
use Amp\Sql\Result;
use Amp\Sql\Statement;

abstract class StatementPool implements Statement
{
    private Pool $pool;

    private \SplQueue $statements;

    private string $sql;

    private int $lastUsedAt;

    private string $timeoutWatcher;

    /** @var callable */
    private $prepare;

    /**
     * Performs any necessary actions to the statement to prepare it for execution, returning a promise for the same or
     * a new Statement object if necessary.
     *
     * @param Statement $statement
     *
     * @return Statement
     */
    abstract protected function prepare(Statement $statement): Statement;

    /**
     * @param Result   $result
     * @param callable $release
     *
     * @return Result
     */
    protected function createResult(Result $result, callable $release): Result
    {
        return new PooledResult($result, $release);
    }

    /**
     * @param Pool      $pool      Pool used to re-create the statement if the original closes.
     * @param Statement $statement Original prepared statement returned from the Link.
     * @param callable  $prepare   Callable that returns a new prepared statement.
     */
    public function __construct(Pool $pool, Statement $statement, callable $prepare)
    {
        $this->lastUsedAt = \time();
        $this->statements = $statements = new \SplQueue;
        $this->pool = $pool;
        $this->prepare = $prepare;
        $this->sql = $statement->getQuery();

        $this->statements->push($statement);

        $this->timeoutWatcher = Loop::repeat(1000, static function () use ($pool, $statements): void {
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

        Loop::unreference($this->timeoutWatcher);
    }

    public function __destruct()
    {
        Loop::cancel($this->timeoutWatcher);
    }

    /**
     * {@inheritdoc}
     *
     * Unlike regular statements, as long as the pool is open this statement will not die.
     */
    public function execute(array $params = []): Result
    {
        $this->lastUsedAt = \time();

        $statement = $this->pop();

        try {
            $statement = $this->prepare($statement);
            $result = $statement->execute($params);
        } catch (\Throwable $exception) {
            $this->push($statement);
            throw $exception;
        }

        return $this->createResult($result, function () use ($statement): void {
            $this->push($statement);
        });
    }

    /**
     * Only retains statements if less than 10% of the pool is consumed by this statement and the pool has
     * available connections.
     *
     * @param Statement $statement
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

        $this->statements->unshift($statement);
    }

    protected function pop(): Statement
    {
        while (!$this->statements->isEmpty()) {
            $statement = $this->statements->pop();

            if ($statement->isAlive()) {
                return $statement;
            }
        }

        return ($this->prepare)($this->sql);
    }

    /** {@inheritdoc} */
    public function isAlive(): bool
    {
        return $this->pool->isAlive();
    }

    /** {@inheritdoc} */
    public function getQuery(): string
    {
        return $this->sql;
    }

    /** {@inheritdoc} */
    public function getLastUsedAt(): int
    {
        return $this->lastUsedAt;
    }
}
