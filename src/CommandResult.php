<?php declare(strict_types=1);

namespace Amp\Sql\Common;

use Amp\Future;
use Amp\Sql\Result;

/**
 * @template TResult of Result
 * @implements \IteratorAggregate<int, never>
 */
final class CommandResult implements Result, \IteratorAggregate
{
    /**
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
     * Always returns null for command results.
     */
    public function fetchRow(): ?array
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
