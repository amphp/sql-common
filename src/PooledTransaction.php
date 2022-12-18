<?php declare(strict_types=1);

namespace Amp\Sql\Common;

use Amp\Sql\Result;
use Amp\Sql\Statement;
use Amp\Sql\Transaction;
use Amp\Sql\TransactionIsolation;

/**
 * @template TResult of Result
 * @template TStatement of Statement
 * @template TTransaction of Transaction
 *
 * @implements Transaction<TResult, TStatement>
 */
abstract class PooledTransaction implements Transaction
{
    /** @var TTransaction  */
    private readonly Transaction $transaction;

    /** @var \Closure():void */
    private readonly \Closure $release;

    private int $refCount = 1;

    /**
     * Creates a Statement of the appropriate type using the Statement object returned by the Transaction object and
     * the given release callable.
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
     *
     * @return TResult
     */
    abstract protected function createResult(Result $result, \Closure $release): Result;

    /**
     * @param TTransaction $transaction Transaction object created by pooled connection.
     * @param \Closure():void $release Callable to be invoked when the transaction completes or is destroyed.
     */
    public function __construct(Transaction $transaction, \Closure $release)
    {
        $this->transaction = $transaction;

        $refCount = &$this->refCount;
        $this->release = static function () use (&$refCount, $release): void {
            if (--$refCount === 0) {
                $release();
            }
        };

        $this->transaction->onClose($this->release);

        if (!$this->transaction->isActive()) {
            $this->close();
        }
    }

    public function query(string $sql): Result
    {
        $result = $this->transaction->query($sql);
        ++$this->refCount;
        return $this->createResult($result, $this->release);
    }

    public function prepare(string $sql): Statement
    {
        $statement = $this->transaction->prepare($sql);
        ++$this->refCount;
        return $this->createStatement($statement, $this->release);
    }

    public function execute(string $sql, array $params = []): Result
    {
        $result = $this->transaction->execute($sql, $params);
        ++$this->refCount;
        return $this->createResult($result, $this->release);
    }

    public function isClosed(): bool
    {
        return $this->transaction->isClosed();
    }

    public function getLastUsedAt(): int
    {
        return $this->transaction->getLastUsedAt();
    }

    /**
     * Rolls back the transaction if it has not been committed.
     */
    public function close(): void
    {
        $this->transaction->close();
    }

    public function onClose(\Closure $onClose): void
    {
        $this->transaction->onClose($onClose);
    }

    public function getIsolationLevel(): TransactionIsolation
    {
        return $this->transaction->getIsolationLevel();
    }

    public function isActive(): bool
    {
        return $this->transaction->isActive();
    }

    public function commit(): void
    {
        $this->transaction->commit();
    }

    public function rollback(): void
    {
        $this->transaction->rollback();
    }

    public function createSavepoint(string $identifier): void
    {
        $this->transaction->createSavepoint($identifier);
    }

    public function rollbackTo(string $identifier): void
    {
        $this->transaction->rollbackTo($identifier);
    }

    public function releaseSavepoint(string $identifier): void
    {
        $this->transaction->releaseSavepoint($identifier);
    }
}
