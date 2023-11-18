<?php declare(strict_types=1);

namespace Amp\Sql\Common;

use Amp\DeferredFuture;
use Amp\Sql\Result;
use Amp\Sql\Statement;
use Amp\Sql\Transaction;
use Amp\Sql\TransactionIsolation;

/**
 * @template TResult of Result
 * @template TStatement of Statement<TResult>
 * @template TTransaction of Transaction
 * @template TNestedTransaction of NestableTransaction<TResult, TStatement, TTransaction>
 *
 * @implements NestableTransaction<TResult, TStatement, TTransaction>
 */
abstract class NestedTransaction implements NestableTransaction
{
    /** @var \Closure():void */
    private readonly \Closure $release;

    private int $refCount = 1;

    private ?DeferredFuture $busy = null;

    private int $nextId = 1;

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
     * @param TNestedTransaction $transaction
     * @param non-empty-string $identifier
     * @param \Closure():void $release
     *
     * @return TTransaction
     */
    abstract protected function createNestedTransaction(
        NestableTransaction $transaction,
        string $identifier,
        \Closure $release,
    ): Transaction;

    /**
     * @param TNestedTransaction $transaction Transaction object created by connection.
     * @param non-empty-string $identifier
     * @param \Closure():void $release Callable to be invoked when the transaction completes or is destroyed.
     */
    public function __construct(
        protected readonly Transaction $transaction,
        private readonly string $identifier,
        \Closure $release,
    ) {
        $busy = &$this->busy;
        $refCount = &$this->refCount;
        $this->release = static function () use (&$busy, &$refCount, $release): void {
            $busy?->complete();
            $busy = null;

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
        $this->awaitPendingNestedTransaction();
        ++$this->refCount;

        try {
            $result = $this->transaction->query($sql);
            return $this->createResult($result, $this->release);
        } catch (\Throwable $exception) {
            $this->release();
            throw $exception;
        }
    }

    public function prepare(string $sql): Statement
    {
        $this->awaitPendingNestedTransaction();
        ++$this->refCount;

        try {
            $statement = $this->transaction->prepare($sql);
            return $this->createStatement($statement, $this->release);
        } catch (\Throwable $exception) {
            $this->release();
            throw $exception;
        }
    }

    public function execute(string $sql, array $params = []): Result
    {
        $this->awaitPendingNestedTransaction();
        ++$this->refCount;

        try {
            $result = $this->transaction->execute($sql, $params);
            return $this->createResult($result, $this->release);
        } catch (\Throwable $exception) {
            $this->release();
            throw $exception;
        }
    }

    public function beginTransaction(): Transaction
    {
        $this->awaitPendingNestedTransaction();
        ++$this->refCount;
        $this->busy = new DeferredFuture();

        $identifier = $this->identifier . '-' . $this->nextId++;

        try {
            $this->transaction->createSavepoint($identifier);
            return $this->createNestedTransaction($this->transaction, $identifier, $this->release);
        } catch (\Throwable $exception) {
            $this->release();
            throw $exception;
        }
    }

    public function isClosed(): bool
    {
        return $this->transaction->isClosed();
    }

    /**
     * Rolls back the transaction if it has not been committed.
     */
    public function close(): void
    {
        $this->awaitPendingNestedTransaction();
        $this->transaction->close();
    }

    public function onClose(\Closure $onClose): void
    {
        $this->transaction->onClose($onClose);
    }

    public function isActive(): bool
    {
        return $this->transaction->isActive();
    }

    public function commit(): void
    {
        $this->awaitPendingNestedTransaction();
        $this->transaction->commit();
    }

    public function rollback(): void
    {
        $this->awaitPendingNestedTransaction();
        $this->transaction->rollback();
    }

    public function createSavepoint(string $identifier): void
    {
        $this->awaitPendingNestedTransaction();
        $this->transaction->createSavepoint($identifier);
    }

    public function releaseSavepoint(string $identifier): void
    {
        $this->awaitPendingNestedTransaction();
        $this->transaction->releaseSavepoint($identifier);
    }

    public function rollbackTo(string $identifier): void
    {
        $this->awaitPendingNestedTransaction();
        $this->transaction->rollbackTo($identifier);
    }

    public function onCommit(\Closure $onCommit): void
    {
        $this->transaction->onCommit($onCommit);
    }

    public function onRollback(\Closure $onRollback): void
    {
        $this->transaction->onRollback($onRollback);
    }

    public function isNestedTransaction(): bool
    {
        return $this->transaction->isNestedTransaction();
    }

    public function getIsolationLevel(): TransactionIsolation
    {
        return $this->transaction->getIsolationLevel();
    }

    public function getLastUsedAt(): int
    {
        return $this->transaction->getLastUsedAt();
    }

    private function awaitPendingNestedTransaction(): void
    {
        while ($this->busy) {
            $this->busy->getFuture()->await();
        }
    }

    private function release(): void
    {
        ($this->release)();
    }
}
