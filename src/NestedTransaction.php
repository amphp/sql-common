<?php declare(strict_types=1);

namespace Amp\Sql\Common;

use Amp\DeferredFuture;
use Amp\Sql\Result;
use Amp\Sql\SqlException;
use Amp\Sql\Statement;
use Amp\Sql\Transaction;
use Amp\Sql\TransactionError;
use Revolt\EventLoop;

/**
 * @template TResult of Result
 * @template TStatement of Statement<TResult>
 * @template TTransaction of Transaction<TResult, TStatement>
 *
 * @extends TransactionDelegate<TResult, TStatement, TTransaction>
 * @implements Transaction<TResult, TStatement>
 */
abstract class NestedTransaction extends TransactionDelegate implements Transaction
{
    private bool $isActive;

    /** @var \Closure():void */
    private readonly \Closure $release;

    private int $refCount = 1;

    private readonly DeferredFuture $onClose;

    /**
     * @param TTransaction $transaction
     * @param \Closure():void $release
     * @param non-empty-string $identifier
     */
    public function __construct(
        Transaction $transaction,
        \Closure $release,
        private readonly string $identifier,
    ) {
        parent::__construct($transaction);

        $refCount = &$this->refCount;
        $this->release = static function () use (&$refCount, $release): void {
            if (--$refCount === 0) {
                $release();
            }
        };

        $this->isActive = $transaction->isActive();
        $this->onClose = new DeferredFuture();
        $this->onClose($this->release);

        if (!$this->isActive) {
            $this->onClose->complete();
        }
    }

    public function __destruct()
    {
        if ($this->onClose->isComplete()) {
            return;
        }

        $this->onClose->complete();

        if (!$this->isActive || !$this->transaction->isActive()) {
            return;
        }

        $this->isActive = false;

        $transaction = $this->transaction;
        $identifier = $this->identifier;
        EventLoop::queue(function () use ($transaction, $identifier): void {
            if (!$transaction->isActive()) {
                return;
            }

            try {
                $transaction->releaseSavepoint($identifier);
            } catch (SqlException) {
                // Ignore failure if connection closes during query.
            }
        });
    }

    /**
     * @param non-empty-string $identifier
     * @return non-empty-string
     */
    protected function makeNestedIdentifier(string $identifier): string
    {
        return $this->identifier . '_' . $identifier;
    }

    public function query(string $sql): Result
    {
        $this->assertActive();
        $result = $this->transaction->query($sql);
        ++$this->refCount;
        return $this->createResult($result, $this->release);
    }

    public function prepare(string $sql): Statement
    {
        $this->assertActive();
        $statement = $this->transaction->prepare($sql);
        ++$this->refCount;
        return $this->createStatement($statement, $this->release);
    }

    public function execute(string $sql, array $params = []): Result
    {
        $this->assertActive();
        $result = $this->transaction->execute($sql, $params);
        ++$this->refCount;
        return $this->createResult($result, $this->release);
    }

    public function isClosed(): bool
    {
        return !$this->isActive;
    }

    public function close(): void
    {
        if ($this->isActive && $this->transaction->isActive()) {
            $this->rollback();
        }
    }

    public function onClose(\Closure $onClose): void
    {
        $this->onClose->getFuture()->finally($onClose);
    }

    public function isActive(): bool
    {
        return $this->isActive;
    }

    public function commit(): void
    {
        $this->assertActive();
        $this->isActive = false;

        try {
            $this->transaction->releaseSavepoint($this->identifier);
        } finally {
            $this->onClose->complete();
        }
    }

    public function rollback(): void
    {
        $this->assertActive();
        $this->isActive = false;

        try {
            $this->transaction->rollbackTo($this->identifier);
        } finally {
            $this->onClose->complete();
        }
    }

    public function createSavepoint(string $identifier): void
    {
        $this->assertActive();
        $this->transaction->createSavepoint($this->makeNestedIdentifier($identifier));
    }

    public function rollbackTo(string $identifier): void
    {
        $this->assertActive();
        $this->transaction->rollbackTo($this->makeNestedIdentifier($identifier));
    }

    public function releaseSavepoint(string $identifier): void
    {
        $this->assertActive();
        $this->transaction->releaseSavepoint($this->makeNestedIdentifier($identifier));
    }

    private function assertActive(): void
    {
        if (!$this->isActive || !$this->transaction->isActive()) {
            throw new TransactionError("The transaction has been committed or rolled back");
        }
    }
}
