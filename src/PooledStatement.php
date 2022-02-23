<?php

namespace Amp\Sql\Common;

use Amp\Sql\Result;
use Amp\Sql\Statement;
use Revolt\EventLoop;

class PooledStatement implements Statement
{
    private readonly Statement $statement;

    /** @var \Closure():void */
    private readonly \Closure $release;

    private int $refCount = 1;

    /**
     * @param Statement $statement Statement object created by pooled connection.
     * @param \Closure():void $release Callable to be invoked when the statement and any associated results are
     *     destroyed.
     */
    public function __construct(Statement $statement, \Closure $release)
    {
        $this->statement = $statement;

        $refCount = &$this->refCount;
        $this->release = static function () use (&$refCount, $release): void {
            if (--$refCount === 0) {
                $release();
            }
        };
    }

    public function __destruct()
    {
        EventLoop::queue($this->release);
    }

    /**
     * Creates a ResultSet of the appropriate type using the ResultSet object returned by the Statement object and
     * the given release callable.
     *
     * @param Result $result
     * @param callable $release
     *
     * @return Result
     */
    protected function createResult(Result $result, callable $release): Result
    {
        return new PooledResult($result, $release);
    }

    public function execute(array $params = []): Result
    {
        $result = $this->statement->execute($params);

        ++$this->refCount;
        return $this->createResult($result, $this->release);
    }

    public function isAlive(): bool
    {
        return $this->statement->isAlive();
    }

    public function getQuery(): string
    {
        return $this->statement->getQuery();
    }

    public function getLastUsedAt(): int
    {
        return $this->statement->getLastUsedAt();
    }
}
