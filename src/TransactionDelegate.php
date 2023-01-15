<?php declare(strict_types=1);

namespace Amp\Sql\Common;

use Amp\Sql\Executor;
use Amp\Sql\Result;
use Amp\Sql\Statement;
use Amp\Sql\Transaction;
use Amp\Sql\TransactionIsolation;

/**
 * @template TResult of Result
 * @template TStatement of Statement<TResult>
 * @template TTransaction of Transaction<TResult, TStatement>
 *
 * @implements Executor<TResult, TStatement>
 */
abstract class TransactionDelegate implements Executor
{
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
     * @param TTransaction $transaction
     */
    public function __construct(protected readonly Transaction $transaction)
    {
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
