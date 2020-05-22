<?php

namespace Amp\Sql\Common;

use Amp\Promise;
use Amp\Sql\Result;
use Amp\Success;

final class CommandResult implements Result
{
    /** @var int */
    private $affectedRows;

    /** @var Promise<Result|null> */
    private $nextResult;

    public function __construct(int $affectedRows, Promise $nextResult)
    {
        $this->affectedRows = $affectedRows;
        $this->nextResult = $nextResult;
    }

    public function continue(): Promise
    {
        return new Success;
    }

    public function dispose(): void
    {
        // No-op
    }

    public function getNextResult(): Promise
    {
        return $this->nextResult;
    }

    public function getRowCount(): int
    {
        return $this->affectedRows;
    }
}
