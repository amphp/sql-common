<?php declare(strict_types=1);

namespace Amp\Sql\Common;

use Amp\Future;
use Amp\Sql\Result;

/**
 * @template TFieldValue
 * @template TResult of Result
 * @implements Result<TFieldValue>
 * @implements \IteratorAggregate<int, never>
 */
abstract class CommandResult implements Result, \IteratorAggregate
{
    /**
     * @param Future<TResult|null> $nextResult
     */
    public function __construct(
        private readonly int $affectedRows,
        private readonly Future $nextResult
    ) {
    }

    final public function getIterator(): \EmptyIterator
    {
        return new \EmptyIterator;
    }

    /**
     * @return null Always returns null for command results.
     */
    final public function fetchRow(): ?array
    {
        return null;
    }

    /**
     * @return TResult|null
     */
    public function getNextResult(): ?Result
    {
        return $this->nextResult->await();
    }

    /**
     * @return int Returns the number of rows affected by the command.
     */
    final public function getRowCount(): int
    {
        return $this->affectedRows;
    }

    /**
     * @return null Always returns null for command results.
     */
    final public function getColumnCount(): ?int
    {
        return null;
    }
}
