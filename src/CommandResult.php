<?php

namespace Amp\Sql\Common;

use Amp\Future;
use Amp\Pipeline\Operator;
use Amp\Pipeline\Pipeline;
use Amp\Sql\Result;
use function Amp\Pipeline\fromIterable;

final class CommandResult implements Result, \IteratorAggregate
{
    private bool $disposed = false;

    /**
     * @param int $affectedRows
     * @param Future<Result|null> $nextResult
     */
    public function __construct(
        private int $affectedRows,
        private Future $nextResult
    ) {
    }

    public function continue(): ?array
    {
        return null;
    }

    public function dispose(): void
    {
        $this->disposed = true;
    }

    public function getIterator(): \Traversable
    {
        return new \EmptyIterator;
    }

    public function onDisposal(callable $onDisposal): void
    {
        // No-op, result is complete on creation
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

    public function pipe(Operator ...$operators): Pipeline
    {
        return fromIterable([])->pipe(...$operators);
    }

    public function isComplete(): bool
    {
        return true;
    }

    public function isDisposed(): bool
    {
        return $this->disposed;
    }
}
