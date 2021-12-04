<?php

namespace Amp\Sql\Common;

use Amp\Future;
use Amp\Sql\Result;
use Revolt\EventLoop;
use function Amp\async;

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
        $this->dispose();
    }

    protected function newInstanceFrom(Result $result, callable $release): self
    {
        return new self($result, $release);
    }

    public function dispose(): void
    {
        if ($this->release !== null) {
            EventLoop::queue($this->release);
            $this->release = null;
        }
    }

    public function getIterator(): \Traversable
    {
        try {
            yield from $this->result;
        } catch (\Throwable $exception) {
            $this->dispose();
            throw $exception;
        }

        if ($this->next === null) {
            $this->next = $this->fetchNextResult();
        }
    }

    public function getRowCount(): ?int
    {
        return $this->result->getRowCount();
    }

    public function getColumnCount(): ?int
    {
        return $this->result->getColumnCount();
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
