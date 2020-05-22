<?php

namespace Amp\Sql\Common;

use Amp\Promise;
use Amp\Sql\Result;
use Amp\Sql\Statement;
use function Amp\call;

class PooledStatement implements Statement
{
    /** @var Statement */
    private $statement;

    /** @var callable|null */
    private $release;

    /** @var int */
    private $refCount = 1;

    /**
     * @param Statement $statement Statement object created by pooled connection.
     * @param callable  $release   Callable to be invoked when the statement and any associated results are destroyed.
     */
    public function __construct(Statement $statement, callable $release)
    {
        $this->statement = $statement;

        if (!$this->statement->isAlive()) {
            $release();
        } else {
            $refCount = &$this->refCount;
            $this->release = static function () use (&$refCount, $release) {
                if (--$refCount === 0) {
                    $release();
                }
            };
        }
    }

    public function __destruct()
    {
        if ($this->release) {
            ($this->release)();
        }
    }

    /**
     * Creates a ResultSet of the appropriate type using the ResultSet object returned by the Statement object and
     * the given release callable.
     *
     * @param Result   $result
     * @param callable $release
     *
     * @return Result
     */
    protected function createResult(Result $result, callable $release): Result
    {
        return new PooledResult($result, $release);
    }

    public function execute(array $params = []): Promise
    {
        return call(function () use ($params) {
            $result = yield $this->statement->execute($params);

            ++$this->refCount;
            return $this->createResult($result, $this->release);
        });
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
