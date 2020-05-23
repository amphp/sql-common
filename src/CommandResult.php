<?php

namespace Amp\Sql\Common;

use Amp\DisposedException;
use Amp\Failure;
use Amp\Promise;
use Amp\Sql\Result;
use Amp\Success;

final class CommandResult implements Result
{
    /** @var int */
    private $affectedRows;

    /** @var Promise<null> */
    private $promise;

    /** @var Promise<Result|null> */
    private $nextResult;

    public function __construct(int $affectedRows, Promise $nextResult)
    {
        $this->affectedRows = $affectedRows;
        $this->promise = new Success;
        $this->nextResult = $nextResult;
    }

    public function continue(): Promise
    {
        return $this->promise;
    }

    public function dispose(): void
    {
        $this->promise = new Failure(new DisposedException);
    }

    public function getNextResult(): Promise
    {
        return $this->nextResult;
    }

    /**
     * @return int Returns the number of rows affected by the command.
     */
    public function getRowCount(): int
    {
        return $this->affectedRows;
    }
}
