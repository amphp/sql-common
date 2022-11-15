<?php

namespace Amp\Sql\Common;

use Amp\Future;
use Amp\Sql\Result;
use Revolt\EventLoop;
use function Amp\async;

/**
 * @template TFieldValue
 * @implements Result<TFieldValue>
 */
abstract class PooledResult implements Result, \IteratorAggregate
{
    private readonly Result $result;

    /** @var null|\Closure():void */
    private ?\Closure $release;

    /** @var Future<Result<TFieldValue>|null>|null */
    private ?Future $next = null;

    /**
     * @param Result<TFieldValue> $result Result object created by pooled connection or statement.
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
     * @param Result<TFieldValue> $result
     * @param \Closure():void $release
     *
     * @return Result<TFieldValue>
     */
    abstract protected function newInstanceFrom(Result $result, \Closure $release): Result;

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

        $this->next ??= $this->fetchNextResult();
    }

    public function fetchRow(): ?array
    {
        return $this->result->fetchRow();
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
     * @return Result<TFieldValue>|null
     */
    public function getNextResult(): ?Result
    {
        return ($this->next ??= $this->fetchNextResult())->await();
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
