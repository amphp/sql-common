<?php

namespace Amp\Sql\Common;

use Amp\Promise;
use Amp\Sql\Result;
use function Amp\async;
use function Amp\await;

class PooledResult implements Result, \IteratorAggregate
{
    private Result $result;

    /** @var callable|null */
    private $release;

    /** @var Promise<Result|null>|null */
    private ?Promise $next = null;

    /**
     * @param Result   $result  Result object created by pooled connection or statement.
     * @param callable $release Callable to be invoked when the result set is destroyed.
     */
    public function __construct(Result $result, callable $release)
    {
        $this->result = $result;
        $this->release = $release;
    }

    public function __destruct()
    {
        if ($this->release !== null) {
            ($this->release)();
        }
    }

    protected function newInstanceFrom(Result $result, callable $release): self
    {
        return new self($result, $release);
    }

    public function continue(): ?array
    {
        try {
            $row = $this->result->continue();
        } catch (\Throwable $exception) {
            $this->dispose();
            throw $exception;
        }

        if ($row === null && $this->next === null) {
            $this->next = $this->fetchNextResult();
        }

        return $row;
    }

    public function dispose(): void
    {
        $this->result->dispose();

        if ($this->release !== null) {
            $release = $this->release;
            $this->release = null;
            $release();
        }
    }

    public function getIterator(): \Iterator
    {
        foreach ($this->result as $value) {
            yield $value;
        }
    }

    public function getRowCount(): ?int
    {
        return $this->result->getRowCount();
    }

    public function getNextResult(): ?Result
    {
        if ($this->next === null) {
            $this->next = $this->fetchNextResult();
        }

        return await($this->next);
    }

    private function fetchNextResult(): Promise
    {
        return async(function (): ?Result {
            $result = $this->result->getNextResult();

            if ($result === null) {
                $this->dispose();
                return null;
            }

            $result = $this->newInstanceFrom($result, $this->release);
            $this->release = null;

            return $result;
        });
    }
}
