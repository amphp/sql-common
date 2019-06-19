<?php

namespace Amp\Sql\Common;

use Amp\Promise;
use Amp\Sql\Link;
use Amp\Sql\Transaction;

class ExtractedConnection implements Link
{
    /** @var Link */
    private $connection;

    /** @var callable */
    private $release;

    public function __construct(Link $link, callable $release)
    {
        $this->connection = $link;
        $this->release = $release;
    }

    public function __destruct()
    {
        ($this->release)();
    }

    public function query(string $sql): Promise
    {
        return $this->connection->query($sql);
    }

    public function prepare(string $sql): Promise
    {
        return $this->connection->prepare($sql);
    }

    public function execute(string $sql, array $params = []): Promise
    {
        return $this->connection->execute($sql, $params);
    }

    public function close()
    {
        return $this->connection->close();
    }

    public function beginTransaction(int $isolation = Transaction::ISOLATION_COMMITTED): Promise
    {
        return $this->connection->beginTransaction($isolation);
    }

    public function isAlive(): bool
    {
        return $this->connection->isAlive();
    }

    public function getLastUsedAt(): int
    {
        return $this->connection->getLastUsedAt();
    }
}
