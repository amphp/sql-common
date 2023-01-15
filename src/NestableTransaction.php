<?php declare(strict_types=1);

namespace Amp\Sql\Common;

use Amp\DeferredFuture;
use Amp\Sql\Link;
use Amp\Sql\Result;
use Amp\Sql\Statement;
use Amp\Sql\Transaction;
use Amp\Sql\TransactionError;
use Amp\Sql\TransactionIsolation;
use Amp\Sql\TransactionIsolationLevel;

/**
 * @template TResult of Result
 * @template TStatement of Statement<TResult>
 * @template TTransaction of Transaction<TResult, TStatement>
 *
 * @implements Link<TResult, TStatement, TTransaction>
 */
abstract class NestableTransaction implements Link
{
    /** @var DeferredFuture<null>|null */
    private ?DeferredFuture $busy = null;

    /** @var \Closure():void */
    private readonly \Closure $release;

    /**
     * @param TTransaction $transaction
     * @param \Closure():void $release
     * @param non-empty-string $identifier
     * @return TTransaction
     */
    abstract protected function createNestedTransaction(
        Transaction $transaction,
        \Closure $release,
        string $identifier,
    ): Transaction;

    /**
     * @param TTransaction $transaction
     */
    public function __construct(
        protected readonly Transaction $transaction,
    ) {
        $busy = &$this->busy;
        $this->release = static function () use (&$busy): void {
            $busy?->complete();
            $busy = null;
        };
    }

    public function query(string $sql): Result
    {
        $this->busy?->getFuture()->await();
        return $this->transaction->query($sql);
    }

    public function prepare(string $sql): Statement
    {
        $this->busy?->getFuture()->await();
        return $this->transaction->prepare($sql);
    }

    public function execute(string $sql, array $params = []): Result
    {
        $this->busy?->getFuture()->await();
        return $this->transaction->execute($sql, $params);
    }

    public function close(): void
    {
        $this->transaction->close();
    }

    public function isClosed(): bool
    {
        return $this->transaction->isClosed();
    }

    public function onClose(\Closure $onClose): void
    {
        $this->transaction->onClose($onClose);
    }

    /**
     * @return TTransaction
     */
    public function beginTransaction(
        TransactionIsolation $isolation = TransactionIsolationLevel::Committed,
    ): Transaction {
        if ($this->transaction->getIsolationLevel()->toSql() !== $isolation->toSql()) {
            throw new TransactionError(
                "Incompatible isolation level in nested transaction; transaction opened using isolation level"
                . " '{$this->transaction->getIsolationLevel()->getLabel()}', nested transaction requested with"
                . " isolation level '{$isolation->getLabel()}'"
            );
        }

        while ($this->busy) {
            $this->busy->getFuture()->await();
        }

        $this->busy = new DeferredFuture();

        try {
            $identifier = \bin2hex(\random_bytes(4));
            $this->transaction->createSavepoint($identifier);

            try {
                return $this->createNestedTransaction($this->transaction, $this->release, $identifier);
            } catch (\Throwable $exception) {
                $this->transaction->rollbackTo($identifier);
                throw $exception;
            }
        } catch (\Throwable $exception) {
            ($this->release)();
            throw $exception;
        }
    }

    public function getIsolationLevel(): TransactionIsolation
    {
        return $this->transaction->getIsolationLevel();
    }

    public function getLastUsedAt(): int
    {
        return $this->transaction->getLastUsedAt();
    }
}
