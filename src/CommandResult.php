<?php

namespace Amp\Sql\Common;

use Amp\Future;
use Amp\Sql\Result;

final class CommandResult implements Result, \IteratorAggregate
{
    /**
     * @param int $affectedRows
     * @param Future<Result|null> $nextResult
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
