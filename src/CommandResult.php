<?php

namespace Amp\Sql\Common;

use Amp\Loop;
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
        // No-op
    }

    public function onDisposal(callable $onDisposal): void
    {
        // No-op, result is complete on creation
    }

    public function onCompletion(callable $onCompletion): void
    {
        try {
            $onCompletion(null);
        } catch (\Throwable $e) {
            Loop::defer(static function () use ($e): void {
                throw $e;
            });
        }
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
