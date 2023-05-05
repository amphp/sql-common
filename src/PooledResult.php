<?php declare(strict_types=1);

namespace Amp\Sql\Common;

use Amp\Future;
use Amp\Sql\Result;
use Revolt\EventLoop;
use function Amp\async;

/**
 * @template TFieldValue
 * @template TResult of Result
 * @implements Result<TFieldValue>
 * @implements \IteratorAggregate<int, array<string, TFieldValue>>
 */
abstract class PooledResult implements Result, \IteratorAggregate
{
    /** @var null|\Closure():void */
    private ?\Closure $release;

    /** @var Future<TResult|null>|null */
    private ?Future $next = null;

    /**
     * @param TResult $result Result object created by pooled connection or statement.
     * @param \Closure():void $release Callable to be invoked when the result set is destroyed.
     */
    public function __construct(private readonly Result $result, \Closure $release)
    {
        $this->release = $release;

        if ($this->result instanceof CommandResult) {
            $this->next = $this->fetchNextResult();
        }
    }

    public function __destruct()
    {
        $this->dispose();
    }

    /**
     * @param TResult $result
     * @param \Closure():void $release
     *
     * @return TResult
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
        } finally {
            $this->next ??= $this->fetchNextResult();
        }
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
     * @return TResult|null
     */
    public function getNextResult(): ?Result
    {
        return ($this->next ??= $this->fetchNextResult())->await();
    }

    private function fetchNextResult(): Future
    {
        return async(function (): ?Result {
            /** @var TResult|null $result */
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
