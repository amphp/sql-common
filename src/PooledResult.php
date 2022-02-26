<?php

namespace Amp\Sql\Common;

use Amp\Future;
use Amp\Sql\Result;
use Revolt\EventLoop;
use function Amp\async;

class PooledResult implements Result, \IteratorAggregate
{
    private readonly Result $result;

    /** @var \Closure():void */
    private readonly \Closure $release;

    /** @var Future<Result|null>|null */
    private ?Future $next = null;

    /**
     * @param Result $result Result object created by pooled connection or statement.
     * @param \Closure():void $release Callable to be invoked when the result set is destroyed.
     */
    public function __construct(Result $result, \Closure $release)
    {
        $this->result = $result;
        $this->release = $release;
    }

    public function __destruct()
    {
        $this->dispose();
    }

    /**
     * @param \Closure():void $release
     */
    protected function newInstanceFrom(Result $result, \Closure $release): self
    {
        return new self($result, $release);
    }

    private function dispose(): void
    {
        if ($this->next === null) {
            $this->next = $this->fetchNextResult();
        }
    }

    public function getIterator(): \Traversable
    {
        try {
            yield from $this->result;
        } finally {
            $this->dispose();
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
                EventLoop::queue($this->release);
                return null;
            }

            return $this->newInstanceFrom($result, $this->release);
        });
    }
}
