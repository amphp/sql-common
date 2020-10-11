<?php

namespace Amp\Sql\Common;

use Amp\Sql\Result;
use Amp\Sql\Statement;
use Amp\Sql\Transaction;
use Amp\Sql\TransactionError;

abstract class PooledTransaction implements Transaction
{
    /** @var Transaction|null */
    private ?Transaction $transaction;

    /** @var callable */
    private $release;

    /** @var int */
    private int $refCount = 1;

    /**
     * Creates a Statement of the appropriate type using the Statement object returned by the Transaction object and
     * the given release callable.
     *
     * @param Statement $statement
     * @param callable  $release
     *
     * @return Statement
     */
    abstract protected function createStatement(Statement $statement, callable $release): Statement;

    /**
     * Creates a ResultSet of the appropriate type using the ResultSet object returned by the Transaction object and
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

    /**
     * @param Transaction $transaction Transaction object created by pooled connection.
     * @param callable    $release     Callable to be invoked when the transaction completes or is destroyed.
     */
    public function __construct(Transaction $transaction, callable $release)
    {
        $this->transaction = $transaction;

        $refCount = &$this->refCount;
        $this->release = static function () use (&$refCount, $release): void {
            if (--$refCount === 0) {
                $release();
            }
        };

        if (!$this->transaction->isActive()) {
            $this->transaction = null;
            ($this->release)();
        }
    }

    public function __destruct()
    {
        if ($this->transaction && $this->transaction->isActive()) {
            $this->close(); // Invokes $this->release callback.
        }
    }

    public function query(string $sql): Result
    {
        if (!$this->transaction) {
            throw new TransactionError("The transaction has been committed or rolled back");
        }

        /** @psalm-suppress PossiblyNullReference $this->transaction checked for null above. */
        $result = $this->transaction->query($sql);

        ++$this->refCount;
        return $this->createResult($result, $this->release);
    }

    public function prepare(string $sql): Statement
    {
        if (!$this->transaction) {
            throw new TransactionError("The transaction has been committed or rolled back");
        }

        /** @psalm-suppress PossiblyNullReference $this->transaction checked for null above. */
        $statement = $this->transaction->prepare($sql);
        ++$this->refCount;
        return $this->createStatement($statement, $this->release);
    }

    public function execute(string $sql, array $params = []): Result
    {
        if (!$this->transaction) {
            throw new TransactionError("The transaction has been committed or rolled back");
        }

        /** @psalm-suppress PossiblyNullReference $this->transaction checked for null above. */
        $result = $this->transaction->execute($sql, $params);

        ++$this->refCount;
        return $this->createResult($result, $this->release);
    }

    public function isAlive(): bool
    {
        return $this->transaction && $this->transaction->isAlive();
    }

    public function getLastUsedAt(): int
    {
        if (!$this->transaction) {
            throw new TransactionError("The transaction has been committed or rolled back");
        }

        return $this->transaction->getLastUsedAt();
    }

    public function close(): void
    {
        if (!$this->transaction) {
            return;
        }

        $transaction = $this->transaction;
        $this->transaction = null;

        $transaction->commit();
        ($this->release)();
    }

    public function getIsolationLevel(): int
    {
        if (!$this->transaction) {
            throw new TransactionError("The transaction has been committed or rolled back");
        }

        return $this->transaction->getIsolationLevel();
    }

    public function isActive(): bool
    {
        return $this->transaction && $this->transaction->isActive();
    }

    public function commit(): void
    {
        if (!$this->transaction) {
            throw new TransactionError("The transaction has been committed or rolled back");
        }

        $transaction = $this->transaction;
        $this->transaction = null;

        $transaction->commit();
        ($this->release)();
    }

    public function rollback(): void
    {
        if (!$this->transaction) {
            throw new TransactionError("The transaction has been committed or rolled back");
        }

        $transaction = $this->transaction;
        $this->transaction = null;

        $transaction->rollback();
        ($this->release)();
    }

    public function createSavepoint(string $identifier): void
    {
        if (!$this->transaction) {
            throw new TransactionError("The transaction has been committed or rolled back");
        }

        $this->transaction->createSavepoint($identifier);
    }

    public function rollbackTo(string $identifier): void
    {
        if (!$this->transaction) {
            throw new TransactionError("The transaction has been committed or rolled back");
        }

        $this->transaction->rollbackTo($identifier);
    }

    public function releaseSavepoint(string $identifier): void
    {
        if (!$this->transaction) {
            throw new TransactionError("The transaction has been committed or rolled back");
        }

        $this->transaction->releaseSavepoint($identifier);
    }
}
