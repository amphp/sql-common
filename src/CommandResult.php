<?php

namespace Amp\Sql\Common;

use Amp\Promise;
use Amp\Sql\Result;
use function Amp\await;

final class CommandResult implements Result
{
    private int $affectedRows;

    /** @var Promise<Result|null> */
    private Promise $nextResult;

    public function __construct(int $affectedRows, Promise $nextResult)
    {
        $this->affectedRows = $affectedRows;
        $this->nextResult = $nextResult;
    }

    public function continue(): ?array
    {
        return null;
    }

    public function dispose(): void
    {
        // No-op
    }

    public function onDisposal(callable $onDisposal): void
    {
        // No-op, result is complete on creation
    }

    public function getNextResult(): ?Result
    {
        return await($this->nextResult);
    }

    /**
     * @return int Returns the number of rows affected by the command.
     */
    public function getRowCount(): int
    {
        return $this->affectedRows;
    }
}
