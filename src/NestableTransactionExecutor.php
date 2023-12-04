<?php declare(strict_types=1);

namespace Amp\Sql\Common;

use Amp\Sql\Executor;
use Amp\Sql\Result;
use Amp\Sql\Statement;

/**
 * @template TResult of Result
 * @template TStatement of Statement<TResult>
 *
 * @extends Executor<TResult, TStatement>
 */
interface NestableTransactionExecutor extends Executor
{
    /**
     * Commits the current transaction.
     */
    public function commit(): void;

    /**
     * Rolls back the current transaction.
     */
    public function rollback(): void;

    /**
     * Creates a savepoint with the given identifier.
     *
     * @param non-empty-string $identifier Savepoint identifier.
     */
    public function createSavepoint(string $identifier): void;

    /**
     * Rolls back to the savepoint with the given identifier.
     *
     * @param non-empty-string $identifier Savepoint identifier.
     */
    public function rollbackTo(string $identifier): void;

    /**
     * Releases the savepoint with the given identifier.
     *
     * @param non-empty-string $identifier Savepoint identifier.
     */
    public function releaseSavepoint(string $identifier): void;
}
