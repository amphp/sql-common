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
        private int $affectedRows,
        private Future $nextResult
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
}
