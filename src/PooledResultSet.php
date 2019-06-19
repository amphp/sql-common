<?php

namespace Amp\Sql\Common;

use Amp\Promise;
use Amp\Sql\ResultSet;

class PooledResultSet implements ResultSet
{
    /** @var ResultSet */
    private $result;

    /** @var callable */
    private $release;

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

    public function advance(): Promise
    {
        $promise = $this->result->advance();

        $promise->onResolve(function (\Throwable $exception = null, bool $moreResults = null) {
            if ($this->release === null) {
                return;
            }

            if ($exception || !$moreResults) {
                $release = $this->release;
                $this->release = null;
                $release();
            }
        });

        return $promise;
    }

    public function getCurrent(): array
    {
        return $this->result->getCurrent();
    }
}
