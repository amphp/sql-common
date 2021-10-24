<?php

namespace Amp\Sql\Common;

use Amp\Future;
use Amp\Pipeline\Operator;
use Amp\Pipeline\Pipeline;
use Amp\Sql\Result;
use Revolt\EventLoop;
use function Amp\coroutine;

class PooledResult implements Result, \IteratorAggregate
{
    private Result $result;

    /** @var callable|null */
    private $release;

    /** @var Future<Result|null>|null */
    private ?Future $next = null;

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
            EventLoop::queue($this->release);
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
            EventLoop::queue($this->release);
            $this->release = null;
        }
    }

    public function getIterator(): \Traversable
    {
        yield from $this->result;
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

        return $this->next->await();
    }

    private function fetchNextResult(): Future
    {
        return coroutine(function (): ?Result {
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

    public function pipe(Operator ...$operators): Pipeline
    {
        return $this->result->pipe(...$operators);
    }

    public function isComplete(): bool
    {
        return $this->result->isComplete();
    }

    public function isDisposed(): bool
    {
        return $this->result->isDisposed();
    }
}
