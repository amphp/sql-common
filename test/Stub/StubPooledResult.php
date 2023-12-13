<?php declare(strict_types=1);

namespace Amp\Sql\Common\Test\Stub;

use Amp\Sql\Common\PooledResult;
use Amp\Sql\Result;

final class StubPooledResult extends PooledResult
{
    protected static function newInstanceFrom(Result $result, \Closure $release): self
    {
        return new self($result, $release);
    }
}
