<?php declare(strict_types=1);

namespace Amp\Sql\Common;

use Amp\ForbidCloning;
use Amp\ForbidSerialization;
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
    use ForbidCloning;
    use ForbidSerialization;

    /** @var Future<TResult|null>|null */
    private ?Future $next = null;

    /** @var \Iterator<int, array<string, TFieldValue>> */
    private readonly \Iterator $iterator;

    /**
     * @template Tr of Result
     *
     * @param Tr $result
     * @param \Closure():void $release
     *
     * @return Tr
     */
    abstract protected static function newInstanceFrom(Result $result, \Closure $release): Result;

    /**
     * @param TResult $result Result object created by pooled connection or statement.
     * @param \Closure():void $release Callable to be invoked when the result set is destroyed.
     */
    public function __construct(private readonly Result $result, private readonly \Closure $release)
    {
        if ($this->result instanceof CommandResult) {
            $this->iterator = $this->result->getIterator();
            $this->next = self::fetchNextResult($this->result, $this->release);
            return;
        }

        $next = &$this->next;
        $this->iterator = (static function () use (&$next, $result, $release): \Generator {
            try {
                yield from $result;
            } catch (\Throwable $exception) {
                if (!$next) {
                    EventLoop::queue($release);
                }
                throw $exception;
            }

            $next ??= self::fetchNextResult($result, $release);
        })();
    }

    public function __destruct()
    {
        EventLoop::queue(self::dispose(...), $this->iterator);
    }

    private static function dispose(\Iterator $iterator): void
    {
        try {
            // Discard remaining rows in the result set.
            while ($iterator->valid()) {
                $iterator->next();
            }
        } catch (\Throwable) {
            // Ignore errors while discarding result.
        }
    }

    public function getIterator(): \Traversable
    {
        return $this->iterator;
    }

    public function fetchRow(): ?array
    {
        if (!$this->iterator->valid()) {
            return null;
        }

        $current = $this->iterator->current();
        $this->iterator->next();
        return $current;
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
        $this->next ??= self::fetchNextResult($this->result, $this->release);
        return $this->next->await();
    }

    /**
     * @template Tr of Result
     *
     * @param Tr $result
     * @param \Closure():void $release
     *
     * @return Future<Tr|null>
     */
    private static function fetchNextResult(Result $result, \Closure $release): Future
    {
        return async(static function () use ($result, $release): ?Result {
            /** @var Tr|null $result */
            $result = $result->getNextResult();

            if ($result === null) {
                EventLoop::queue($release);
                return null;
            }

            return static::newInstanceFrom($result, $release);
        });
    }
}
