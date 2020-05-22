<?php

namespace Amp\Sql\Common;

use Amp\Promise;
use Amp\Sql\Result;
use function Amp\call;

class PooledResult implements Result
{
    /** @var Result */
    private $result;

    /** @var callable */
    private $release;

    /** @var Promise<Result|null>|null */
    private $next;

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

    public function continue(): Promise
    {
        $promise = $this->result->continue();

        $promise->onResolve(function (?\Throwable $exception, ?array $row): void {
            if ($this->release === null) {
                return;
            }

            if ($exception) {
                $this->dispose();
                return;
            }

            if ($row === null && $this->next === null) {
                $this->next = $this->fetchNextResult();
            }
        });

        return $promise;
    }

    public function dispose(): void
    {
        $this->result->dispose();

        $release = $this->release;
        $this->release = null;
        $release();
    }

    public function getRowCount(): ?int
    {
        return $this->result->getRowCount();
    }

    public function getNextResult(): Promise
    {
        if ($this->next === null) {
            $this->next = $this->fetchNextResult();
        }

        return $this->next;
    }

    private function fetchNextResult(): Promise
    {
        return call(function () {
            $result = yield $this->result->getNextResult();

            if ($this->release === null) {
                return null;
            }

            if ($result === null) {
                $this->dispose();
                return null;
            }

            $result = new self($result, $this->release);
            $this->release = null;

            return $result;
        });
    }
}
