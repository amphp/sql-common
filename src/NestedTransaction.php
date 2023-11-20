<?php declare(strict_types=1);

namespace Amp\Sql\Common;

use Amp\DeferredFuture;
use Amp\Sql\Executor;
use Amp\Sql\Result;
use Amp\Sql\Statement;
use Amp\Sql\Transaction;
use Amp\Sql\TransactionError;
use Amp\Sql\TransactionIsolation;
use Revolt\EventLoop;

/**
 * @template TResult of Result
 * @template TStatement of Statement<TResult>
 * @template TTransaction of Transaction
 * @template TNestedExecutor of NestableTransactionExecutor<TResult, TStatement, TTransaction>
 *
 * @implements Transaction<TResult, TStatement, TTransaction>
 */
abstract class NestedTransaction implements Transaction
{
    /** @var \Closure():void */
    private readonly \Closure $release;

    private int $refCount = 1;

    private bool $active = true;

    private ?DeferredFuture $busy = null;

    private DeferredFuture $onClose;

    private DeferredFuture $onRollback;

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
     * @param TNestedExecutor $executor
     * @param non-empty-string $identifier
     * @param \Closure():void $release
     *
     * @return TTransaction
     */
    abstract protected function createNestedTransaction(
        NestableTransactionExecutor $executor,
        string $identifier,
        \Closure $release,
    ): Transaction;

    /**
     * @param TTransaction $transaction Transaction object created by connection.
     * @param TNestedExecutor $executor
     * @param non-empty-string $identifier
     * @param \Closure():void $release Callable to be invoked when the transaction completes or is destroyed.
     */
    public function __construct(
        protected readonly Transaction $transaction,
        protected readonly Executor $executor,
        private readonly string $identifier,
        \Closure $release,
    ) {
        $this->onRollback = new DeferredFuture();
        $this->onClose = new DeferredFuture();

        $busy = &$this->busy;
        $refCount = &$this->refCount;
        $this->release = static function () use (&$busy, &$refCount, $release): void {
            $busy?->complete();
            $busy = null;

            if (--$refCount === 0) {
                $release();
            }
        };

        if (!$this->transaction->isActive()) {
            $this->active = false;
            EventLoop::queue($release);
        }
    }

    public function query(string $sql): Result
    {
        $this->awaitPendingNestedTransaction();
        ++$this->refCount;

        try {
            $result = $this->executor->query($sql);
            return $this->createResult($result, $this->release);
        } catch (\Throwable $exception) {
            EventLoop::queue($this->release);
            throw $exception;
        }
    }

    public function prepare(string $sql): Statement
    {
        $this->awaitPendingNestedTransaction();
        ++$this->refCount;

        try {
            $statement = $this->executor->prepare($sql);
            return $this->createStatement($statement, $this->release);
        } catch (\Throwable $exception) {
            EventLoop::queue($this->release);
            throw $exception;
        }
    }

    public function execute(string $sql, array $params = []): Result
    {
        $this->awaitPendingNestedTransaction();
        ++$this->refCount;

        try {
            $result = $this->executor->execute($sql, $params);
            return $this->createResult($result, $this->release);
        } catch (\Throwable $exception) {
            EventLoop::queue($this->release);
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
            $this->executor->createSavepoint($identifier);
            return $this->createNestedTransaction($this->executor, $identifier, $this->release);
        } catch (\Throwable $exception) {
            EventLoop::queue($this->release);
            throw $exception;
        }
    }

    public function isClosed(): bool
    {
        return $this->executor->isClosed();
    }

    /**
     * Rolls back the transaction if it has not been committed.
     */
    public function close(): void
    {
        if ($this->active) {
            $this->rollback();
        }
    }

    public function onClose(\Closure $onClose): void
    {
        $this->executor->onClose($onClose);
    }

    public function isActive(): bool
    {
        return !$this->isClosed();
    }

    public function commit(): void
    {
        $this->awaitPendingNestedTransaction();
        $this->active = false;

        $this->executor->releaseSavepoint($this->identifier);
        EventLoop::queue($this->release);

        $onRollback = $this->onRollback;
        $this->transaction->onRollback(static fn () => $onRollback->isComplete() || $onRollback->complete());
        $this->onClose->complete();
    }

    public function rollback(): void
    {
        $this->awaitPendingNestedTransaction();
        $this->active = false;

        $this->executor->rollbackTo($this->identifier);
        EventLoop::queue($this->release);

        $this->onRollback->complete();
        $this->onClose->complete();
    }

    public function onCommit(\Closure $onCommit): void
    {
        $this->transaction->onCommit($onCommit);
    }

    public function onRollback(\Closure $onRollback): void
    {
        if ($this->active) {
            $this->onRollback->getFuture()->finally($onRollback);
            return;
        }

        $this->transaction->onRollback($onRollback);
    }

    public function isNestedTransaction(): bool
    {
        return true;
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

        if (!$this->active) {
            throw new TransactionError('The transaction has already been committed or rolled back');
        }
    }
}
