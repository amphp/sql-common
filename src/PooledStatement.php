<?php declare(strict_types=1);

namespace Amp\Sql\Common;

use Amp\Sql\Result;
use Amp\Sql\SqlException;
use Amp\Sql\Statement;
use Revolt\EventLoop;

/**
 * @template TResult of Result
 * @template TStatement of Statement<TResult>
 *
 * @implements Statement<TResult>
 */
abstract class PooledStatement implements Statement
{
    /** @var TStatement */
    private readonly Statement $statement;

    /** @var null|\Closure():void */
    private ?\Closure $release;

    private int $refCount = 1;

    /**
     * Creates a Result of the appropriate type using the Result object returned by the Statement object and the
     * given release callable.
     *
     * @param TResult $result
     * @param \Closure():void $release
     *
     * @return TResult
     */
    abstract protected function createResult(Result $result, \Closure $release): Result;

    /**
     * @param TStatement $statement Statement object created by pooled connection.
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
        $this->dispose();
    }

    /**
     * @return TResult
     */
    public function execute(array $params = []): Result
    {
        if (!$this->release) {
            throw new SqlException('The statement has been closed');
        }

        $result = $this->statement->execute($params);

        ++$this->refCount;
        return $this->createResult($result, $this->release);
    }

    private function dispose(): void
    {
        if ($this->release) {
            EventLoop::queue($this->release);
            $this->release = null;
        }
    }

    public function isClosed(): bool
    {
        return $this->statement->isClosed();
    }

    public function close(): void
    {
        $this->dispose();
        $this->statement->close();
    }

    public function onClose(\Closure $onClose): void
    {
        $this->statement->onClose($onClose);
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
