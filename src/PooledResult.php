<?php

namespace Amp\Sql\Common;

use Amp\Future;
use Amp\Sql\Result;
use Revolt\EventLoop;
use function Amp\async;

/**
 * @template TResult extends Result
 */
abstract class PooledResult implements Result, \IteratorAggregate
{
    private readonly Result $result;

    /** @var null|\Closure():void */
    private ?\Closure $release;

    /** @var Future<TResult|null>|null */
    private ?Future $next = null;

    /**
     * @param TResult $result Result object created by pooled connection or statement.
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
     * @param TResult $result
     * @param \Closure():void $release
     *
     * @return PooledResult<TResult>
     */
    abstract protected function newInstanceFrom(Result $result, \Closure $release): self;

    private function dispose(): void
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

    /**
     * @return TResult|null
     */
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

            if ($result === null || $this->release === null) {
                $this->dispose();
                return null;
            }

            $result = $this->newInstanceFrom($result, $this->release);
            $this->release = null;

            return $result;
        });
    }
}
