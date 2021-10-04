<?php

declare(strict_types=1);

namespace Doctrine\DBAL\Tests\Functional;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Exception\UniqueConstraintViolationException;
use Doctrine\DBAL\Schema\Constraint;
use Doctrine\DBAL\Schema\Table;
use Doctrine\DBAL\Schema\UniqueConstraint;
use Doctrine\DBAL\Tests\FunctionalTestCase;
use Throwable;

use function sprintf;

final class DeferrableConstraintsTest extends FunctionalTestCase
{
    /** @var string */
    private $constraintName = '';

    protected function setUp(): void
    {
        parent::setUp();

        $platformName = $this->connection->getDatabasePlatform()->getName();

        if ($platformName === 'oracle') {
            $constraintName = 'C1_UNIQUE';
        } elseif ($platformName === 'postgresql') {
            $constraintName = 'c1_unique';
        } else {
            $constraintName = 'c1_unique';
        }

        $this->constraintName = $constraintName;

        $schemaManager = $this->connection->createSchemaManager();

        $table = new Table('deferrable_constraints');
        $table->addColumn('unique_field', 'integer', ['notnull' => true]);
        $schemaManager->createTable($table);

        if ($platformName === 'oracle') {
            $createConstraint = sprintf(
                'ALTER TABLE deferrable_constraints ' .
                'ADD CONSTRAINT %s UNIQUE (unique_field) DEFERRABLE INITIALLY IMMEDIATE',
                $constraintName
            );
        } elseif ($platformName === 'postgresql') {
            $createConstraint = sprintf(
                'ALTER TABLE deferrable_constraints ' .
                'ADD CONSTRAINT %s UNIQUE (unique_field) DEFERRABLE INITIALLY IMMEDIATE',
                $constraintName
            );
        } elseif ($platformName === 'sqlite') {
            $createConstraint = sprintf(
                'CREATE UNIQUE INDEX %s ON deferrable_constraints(unique_field)',
                $constraintName
            );
        } else {
            $createConstraint = new UniqueConstraint($constraintName, ['unique_field']);
        }

        if ($createConstraint instanceof UniqueConstraint) {
            $schemaManager->createUniqueConstraint($createConstraint, 'deferrable_constraints');
        } else {
            $this->connection->executeStatement($createConstraint);
        }

        $this->connection->executeStatement('INSERT INTO deferrable_constraints VALUES (1)');
    }

    public function testTransactionalWithDeferredConstraint(): void
    {
        $this->skipIfDeferrableIsNotSupported();

        $this->connection->transactional(function (Connection $connection): void {
            $connection->executeStatement(sprintf('SET CONSTRAINTS "%s" DEFERRED', $this->constraintName));

            $connection->executeStatement('INSERT INTO deferrable_constraints VALUES (1)');

            $this->expectException(UniqueConstraintViolationException::class);
        });
    }

    public function testTransactionalWithNonDeferredConstraint(): void
    {
        $this->connection->transactional(function (Connection $connection): void {
            $this->expectException(UniqueConstraintViolationException::class);
            $connection->executeStatement('INSERT INTO deferrable_constraints VALUES (1)');
        });
    }

    public function testTransactionalWithDeferredConstraintAndTransactionNesting(): void
    {
        $this->skipIfDeferrableIsNotSupported();

        $this->connection->setNestTransactionsWithSavepoints(true);

        $this->connection->transactional(function (Connection $connection): void {
            $connection->executeStatement(sprintf('SET CONSTRAINTS "%s" DEFERRED', $this->constraintName));
            $connection->beginTransaction();
            $connection->executeStatement('INSERT INTO deferrable_constraints VALUES (1)');
            $connection->commit();

            $this->expectException(UniqueConstraintViolationException::class);
        });
    }

    public function testTransactionalWithNonDeferredConstraintAndTransactionNesting(): void
    {
        $this->connection->setNestTransactionsWithSavepoints(true);

        $this->connection->transactional(function (Connection $connection): void {
            $connection->beginTransaction();

            try {
                $this->connection->executeStatement('INSERT INTO deferrable_constraints VALUES (1)');
            } catch (Throwable $t) {
                $this->connection->rollBack();

                $this->expectException(UniqueConstraintViolationException::class);

                throw $t;
            }
        });
    }

    public function testCommitWithDeferredConstraint(): void
    {
        $this->skipIfDeferrableIsNotSupported();

        $this->connection->beginTransaction();
        $this->connection->executeStatement(sprintf('SET CONSTRAINTS "%s" DEFERRED', $this->constraintName));
        $this->connection->executeStatement('INSERT INTO deferrable_constraints VALUES (1)');

        $this->expectException(UniqueConstraintViolationException::class);
        $this->connection->commit();
    }

    public function testInsertWithNonDeferredConstraint(): void
    {
        $this->connection->beginTransaction();

        try {
            $this->connection->executeStatement('INSERT INTO deferrable_constraints VALUES (1)');
        } catch (Throwable $t) {
            $this->connection->rollBack();

            $this->expectException(UniqueConstraintViolationException::class);

            throw $t;
        }
    }

    public function testCommitWithDeferredConstraintAndTransactionNesting(): void
    {
        $this->skipIfDeferrableIsNotSupported();

        $this->connection->setNestTransactionsWithSavepoints(true);

        $this->connection->beginTransaction();
        $this->connection->executeStatement(sprintf('SET CONSTRAINTS "%s" DEFERRED', $this->constraintName));
        $this->connection->beginTransaction();
        $this->connection->executeStatement('INSERT INTO deferrable_constraints VALUES (1)');
        $this->connection->commit();

        $this->expectException(UniqueConstraintViolationException::class);

        $this->connection->commit();
    }

    public function testCommitWithNonDeferredConstraintAndTransactionNesting(): void
    {
        $this->skipIfDeferrableIsNotSupported();

        $this->connection->setNestTransactionsWithSavepoints(true);

        $this->connection->beginTransaction();
        $this->connection->beginTransaction();

        try {
            $this->connection->executeStatement('INSERT INTO deferrable_constraints VALUES (1)');
        } catch (Throwable $t) {
            $this->connection->rollBack();

            $this->expectException(UniqueConstraintViolationException::class);

            throw $t;
        }
    }

    private function supportsDeferrableConstraints(): bool
    {
        $platformName = $this->connection->getDatabasePlatform()->getName();

        switch ($platformName) {
            case 'oracle':
            case 'postgresql':
                return true;

            default:
                return false;
        }
    }

    private function skipIfDeferrableIsNotSupported(): void
    {
        if ($this->supportsDeferrableConstraints()) {
            return;
        }

        self::markTestSkipped('Only databases supporting deferrable constraints are eligible for this test.');
    }

    protected function tearDown(): void
    {
        $schemaManager = $this->connection->createSchemaManager();
        $schemaManager->dropTable('deferrable_constraints');

        $this->markConnectionNotReusable();

        parent::tearDown();
    }
}
