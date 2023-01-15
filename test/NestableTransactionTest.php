<?php declare(strict_types=1);

namespace Amp\Sql\Common\Test;

use Amp\PHPUnit\AsyncTestCase;
use Amp\Sql\Common\NestableTransaction;
use Amp\Sql\Common\NestedTransaction;
use Amp\Sql\Transaction;
use Amp\Sql\TransactionError;
use Amp\Sql\TransactionIsolationLevel;
use Amp\TimeoutCancellation;
use PHPUnit\Framework\MockObject\MockObject;
use function Amp\async;

class NestableTransactionTest extends AsyncTestCase
{
    public function createTransaction(): MockObject&Transaction
    {
        $transaction = $this->createMock(Transaction::class);
        $transaction->method('getIsolationLevel')
            ->willReturn(TransactionIsolationLevel::Committed);
        $transaction->method('isActive')
            ->willReturn(true);

        return $transaction;
    }

    public function createNestableTransaction(Transaction $transaction): MockObject&NestableTransaction
    {
        $nestable = $this->getMockForAbstractClass(NestableTransaction::class, [$transaction]);
        $nestable->method('createNestedTransaction')
            ->willReturnCallback(function (
                Transaction $transaction,
                \Closure $release,
                string $identifier
            ): NestedTransaction {
                return $this->getMockForAbstractClass(NestedTransaction::class, [$transaction, $release, $identifier]);
            });

        return $nestable;
    }

    public function testBeginTransaction(): void
    {
        $transaction = $this->createTransaction();
        $transaction->expects(self::once())
            ->method('createSavepoint');

        $nestable = $this->createNestableTransaction($transaction);
        $nested = $nestable->beginTransaction();

        self::assertInstanceOf(NestedTransaction::class, $nested);
    }

    public function testCommit(): void
    {
        $transaction = $this->createTransaction();
        $transaction->expects(self::once())
            ->method('createSavepoint');
        $transaction->expects(self::once())
            ->method('releaseSavepoint');

        $nestable = $this->createNestableTransaction($transaction);
        $nested = $nestable->beginTransaction();

        self::assertFalse($nested->isClosed());
        $nested->onClose($this->createCallback(1));

        $nested->commit();

        self::assertTrue($nested->isClosed());
    }

    /**
     * @depends testCommit
     */
    public function testDoubleCommit(): void
    {
        $nestable = $this->createNestableTransaction($this->createTransaction());
        $nested = $nestable->beginTransaction();

        $nested->commit();

        $this->expectException(TransactionError::class);

        $nested->commit();
    }

    public function testRollback(): void
    {
        $transaction = $this->createTransaction();
        $transaction->expects(self::once())
            ->method('createSavepoint');
        $transaction->expects(self::once())
            ->method('rollbackTo');

        $nestable = $this->createNestableTransaction($transaction);
        $nested = $nestable->beginTransaction();

        self::assertFalse($nested->isClosed());
        $nested->onClose($this->createCallback(1));

        $nested->rollback();

        self::assertTrue($nested->isClosed());
    }

    /**
     * @depends testRollback
     */
    public function testDoubleRollback(): void
    {
        $nestable = $this->createNestableTransaction($this->createTransaction());
        $nested = $nestable->beginTransaction();

        $nested->rollback();

        $this->expectException(TransactionError::class);

        $nested->rollback();
    }

    public function testDifferentIsolationLevel(): void
    {
        $nestable = $this->createNestableTransaction($this->createTransaction());

        $this->expectException(TransactionError::class);
        $this->expectExceptionMessage('Incompatible isolation level');

        $nestable->beginTransaction(TransactionIsolationLevel::Serializable);
    }

    public function testBusy(): void
    {
        $transaction = $this->createTransaction();
        $transaction->expects(self::exactly(3))
            ->method('createSavepoint');

        $nestable = $this->createNestableTransaction($transaction);

        $future1 = async($nestable->beginTransaction(...));
        $future2 = async($nestable->beginTransaction(...));
        $future3 = async($nestable->beginTransaction(...));

        /** @var Transaction $nested1 */
        $nested1 = $future1->await(new TimeoutCancellation(0));

        self::assertFalse($future2->isComplete());
        self::assertFalse($future3->isComplete());

        $nested1->onClose($this->createCallback(1));
        $nested1->close();

        /** @var Transaction $nested2 */
        $nested2 = $future2->await(new TimeoutCancellation(0));

        self::assertFalse($future3->isComplete());

        $nested2->onClose($this->createCallback(1));
        $nested2->close();

        /** @var Transaction $nested3 */
        $nested3 = $future3->await(new TimeoutCancellation(0));

        $nested3->onClose($this->createCallback(1));
        $nested3->close();
    }
}
