<?php declare(strict_types=1);

namespace Amp\Sql\Common;

use Amp\DeferredFuture;
use Amp\Sql\Result;
use Amp\Sql\SqlException;
use Amp\Sql\Statement;
use Amp\Sql\Transaction;
use Amp\Sql\TransactionError;
use Amp\Sql\TransactionIsolation;
use Revolt\EventLoop;

/**
 * @template TResult of Result
 * @template TStatement of Statement<TResult>
 * @template TTransaction of Transaction
 * @template TNestedExecutor of NestableTransactionExecutor<TResult, TStatement>
 *
 * @implements Transaction<TResult, TStatement, TTransaction>
 */
abstract class NestedTransaction implements Transaction
{
    /** @var \Closure():void */
    private readonly \Closure $release;

    private int $refCount = 1;

    private bool $active = true;

    private readonly DeferredFuture $onCommit;
    private readonly DeferredFuture $onRollback;
    private readonly DeferredFuture $onClose;

    private ?DeferredFuture $busy = null;

    private int $nextId = 1;

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
     * @param TTransaction $transaction
     * @param TNestedExecutor $executor
     * @param non-empty-string $identifier
     * @param \Closure():void $release
     *
     * @return TTransaction
     */
    abstract protected function createNestedTransaction(
        Transaction $transaction,
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
        private readonly Transaction $transaction,
        private readonly NestableTransactionExecutor $executor,
        private readonly string $identifier,
        \Closure $release,
    ) {
        $this->onCommit = new DeferredFuture();
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

        $this->onClose($this->release);

        if (!$this->transaction->isActive()) {
            $this->active = false;
            $this->onClose->complete();
        }
    }

    public function __destruct()
    {
        if ($this->onClose->isComplete()) {
            return;
        }

        $this->onClose->complete();

        if ($this->executor->isClosed()) {
            return;
        }

        $busy = &$this->busy;
        $transaction = $this->transaction;
        $executor = $this->executor;
        $identifier = $this->identifier;
        $onRollback = $this->onRollback;
        $onClose = $this->onClose;
        EventLoop::queue(static function () use (
            &$busy,
            $transaction,
            $executor,
            $identifier,
            $onRollback,
            $onClose,
        ): void {
            try {
                while ($busy) {
                    $busy->getFuture()->await();
                }

                if ($transaction->isActive() && !$executor->isClosed()) {
                    $executor->rollbackTo($identifier);
                }
            } catch (SqlException) {
                // Ignore failure if connection closes during query.
            } finally {
                $transaction->onRollback(static fn () => $onRollback->isComplete() || $onRollback->complete());
                $onClose->complete();
            }
        });
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

        return $this->executor->prepare($sql);
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
            return $this->createNestedTransaction($this->transaction, $this->executor, $identifier, $this->release);
        } catch (\Throwable $exception) {
            EventLoop::queue($this->release);
            throw $exception;
        }
    }

    public function isClosed(): bool
    {
        return $this->onClose->isComplete();
    }

    /**
     * Rolls back the transaction if it has not been committed.
     */
    public function close(): void
    {
        if (!$this->active) {
            return;
        }

        $this->rollback();
    }

    public function onClose(\Closure $onClose): void
    {
        $this->onClose->getFuture()->finally($onClose);
    }

    public function isActive(): bool
    {
        return $this->active && $this->transaction->isActive();
    }

    public function commit(): void
    {
        $this->active = false;
        $this->awaitPendingNestedTransaction();

        try {
            $this->executor->releaseSavepoint($this->identifier);
        } finally {
            $onCommit = $this->onCommit;
            $this->transaction->onCommit(static fn () => $onCommit->isComplete() || $onCommit->complete());

            $onRollback = $this->onRollback;
            $this->transaction->onRollback(static fn () => $onRollback->isComplete() || $onRollback->complete());

            $this->onClose->complete();
        }
    }

    public function rollback(): void
    {
        $this->active = false;
        $this->awaitPendingNestedTransaction();

        try {
            $this->executor->rollbackTo($this->identifier);
        } finally {
            $this->onRollback->complete();
            $this->onClose->complete();
        }
    }

    public function onCommit(\Closure $onCommit): void
    {
        $this->onCommit->getFuture()->finally($onCommit);
    }

    public function onRollback(\Closure $onRollback): void
    {
        $this->onRollback->getFuture()->finally($onRollback);
    }

    public function getSavepointIdentifier(): string
    {
        return $this->identifier;
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

        if ($this->isClosed()) {
            throw new TransactionError('The transaction has already been committed or rolled back');
        }
    }
}
