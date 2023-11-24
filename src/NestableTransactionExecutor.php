<?php declare(strict_types=1);

namespace Amp\Sql\Common;

use Amp\Sql\Executor;
use Amp\Sql\Result;
use Amp\Sql\Statement;
use Amp\Sql\Transaction;
use Amp\Sql\TransactionError;

/**
 * @template TResult of Result
 * @template TStatement of Statement<TResult>
 *
 * @extends Executor<TResult, TStatement>
 */
interface NestableTransactionExecutor extends Executor
{
    /**
     * Creates a savepoint with the given identifier.
     *
     * @param non-empty-string $identifier Savepoint identifier.
     *
     * @throws TransactionError If the transaction has been committed or rolled back.
     */
    public function createSavepoint(string $identifier): void;

    /**
     * Rolls back to the savepoint with the given identifier.
     *
     * @param non-empty-string $identifier Savepoint identifier.
     *
     * @throws TransactionError If the transaction has been committed or rolled back.
     */
    public function rollbackTo(string $identifier): void;

    /**
     * Releases the savepoint with the given identifier.
     *
     * @param non-empty-string $identifier Savepoint identifier.
     *
     * @throws TransactionError If the transaction has been committed or rolled back.
     */
    public function releaseSavepoint(string $identifier): void;
}
