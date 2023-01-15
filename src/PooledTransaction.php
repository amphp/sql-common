<?php declare(strict_types=1);

namespace Amp\Sql\Common;

use Amp\Sql\Result;
use Amp\Sql\Statement;
use Amp\Sql\Transaction;

/**
 * @template TResult of Result
 * @template TStatement of Statement<TResult>
 * @template TTransaction of Transaction<TResult, TStatement>
 *
 * @extends TransactionDelegate<TResult, TStatement, TTransaction>
 * @implements Transaction<TResult, TStatement>
 */
abstract class PooledTransaction extends TransactionDelegate implements Transaction
{
    /** @var \Closure():void */
    private readonly \Closure $release;

    private int $refCount = 1;

    /**
     * @param TTransaction $transaction Transaction object created by pooled connection.
     * @param \Closure():void $release Callable to be invoked when the transaction completes or is destroyed.
     */
    public function __construct(Transaction $transaction, \Closure $release)
    {
        parent::__construct($transaction);

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
