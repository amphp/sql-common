<?php

namespace Amp\Sql\Common;

use Amp\Promise;
use Amp\Sql\ResultSet;
use function Amp\call;

abstract class PooledResultSet implements ResultSet
{
    /** @var ResultSet */
    private $result;

    /** @var callable */
    private $release;

    /** @var Promise<ResultSet|null>|null */
    private $next;

    /**
     * Creates a new instance from the given result set and release callable.
     *
     * @param ResultSet $result
     * @param callable  $release
     *
     * @return self
     */
    abstract protected function createNewInstanceFrom(ResultSet $result, callable $release): self;

    /**
     * @param ResultSet $result ResultSet object created by pooled connection or statement.
     * @param callable  $release Callable to be invoked when the result set is destroyed.
     */
    public function __construct(ResultSet $result, callable $release)
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
                $this->next = $this->fetchNextResultSet();
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

    public function getNextResultSet(): Promise
    {
        if ($this->next === null) {
            $this->next = $this->fetchNextResultSet();
        }

        return $this->next;
    }

    private function fetchNextResultSet(): Promise
    {
        return call(function () {
            $result = yield $this->result->getNextResultSet();

            if ($this->release === null) {
                return null;
            }

            if ($result === null) {
                $this->dispose();
                return null;
            }

            $result = $this->createNewInstanceFrom($result, $this->release);
            $this->release = null;

            return $result;
        });
    }
}
