<?php

namespace Amp\Sql\Common;

use Amp\Future;
use Amp\Sql\Result;

/**
 * @template TResult extends Result
 */
final class CommandResult implements Result, \IteratorAggregate
{
    /**
     * @param int $affectedRows
     * @param Future<TResult|null> $nextResult
     */
    public function __construct(
        private readonly int $affectedRows,
        private readonly Future $nextResult
    ) {
    }

    public function getIterator(): \Traversable
    {
        return new \EmptyIterator;
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
    public function getRowCount(): int
    {
        return $this->affectedRows;
    }

    /**
     * @return int|null Always returns null for command results.
     */
    public function getColumnCount(): ?int
    {
        return null;
    }
}
