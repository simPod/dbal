<?php

declare(strict_types=1);

namespace Doctrine\DBAL\Tests\Functional;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Exception\DriverException;
use Doctrine\DBAL\Exception\UniqueConstraintViolationException;
use Doctrine\DBAL\Platforms\DB2Platform;
use Doctrine\DBAL\Platforms\OraclePlatform;
use Doctrine\DBAL\Platforms\PostgreSQLPlatform;
use Doctrine\DBAL\Platforms\SqlitePlatform;
use Doctrine\DBAL\Platforms\SQLServerPlatform;
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

        $platform = $this->connection->getDatabasePlatform();

        if ($platform instanceof OraclePlatform) {
            $constraintName = 'C1_UNIQUE';
        } elseif ($platform instanceof PostgreSQLPlatform) {
            $constraintName = 'c1_unique';
        } else {
            $constraintName = 'c1_unique';
        }

        $this->constraintName = $constraintName;

        $schemaManager = $this->connection->createSchemaManager();

        $table = new Table('deferrable_constraints');
        $table->addColumn('unique_field', 'integer', ['notnull' => true]);
        $schemaManager->createTable($table);

        if ($platform instanceof OraclePlatform) {
            $createConstraint = sprintf(
                'ALTER TABLE deferrable_constraints ' .
                'ADD CONSTRAINT %s UNIQUE (unique_field) DEFERRABLE INITIALLY IMMEDIATE',
                $constraintName
            );
        } elseif ($platform instanceof PostgreSQLPlatform) {
            $createConstraint = sprintf(
                'ALTER TABLE deferrable_constraints ' .
                'ADD CONSTRAINT %s UNIQUE (unique_field) DEFERRABLE INITIALLY IMMEDIATE',
                $constraintName
            );
        } elseif ($platform instanceof SqlitePlatform) {
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

            $this->expectUniqueConstraintViolation();
        });
    }

    public function testTransactionalWithNonDeferredConstraint(): void
    {
        $this->connection->transactional(function (Connection $connection): void {
            $this->expectUniqueConstraintViolation();
            $connection->executeStatement('INSERT INTO deferrable_constraints VALUES (1)');
        });
    }

    public function testTransactionalWithDeferredConstraintAndTransactionNesting(): void
    {
        if (! $this->connection->getDatabasePlatform()->supportsSavepoints()) {
            self::markTestSkipped('This test requires the platform to support savepoints.');
        }

        $this->skipIfDeferrableIsNotSupported();

        $this->connection->setNestTransactionsWithSavepoints(true);

        $this->connection->transactional(function (Connection $connection): void {
            $connection->executeStatement(sprintf('SET CONSTRAINTS "%s" DEFERRED', $this->constraintName));
            $connection->beginTransaction();
            $connection->executeStatement('INSERT INTO deferrable_constraints VALUES (1)');
            $connection->commit();

            $this->expectUniqueConstraintViolation();
        });
    }

    public function testTransactionalWithNonDeferredConstraintAndTransactionNesting(): void
    {
        if (! $this->connection->getDatabasePlatform()->supportsSavepoints()) {
            self::markTestSkipped('This test requires the platform to support savepoints.');
        }

        $this->connection->setNestTransactionsWithSavepoints(true);

        $this->connection->transactional(function (Connection $connection): void {
            $connection->beginTransaction();

            try {
                $this->connection->executeStatement('INSERT INTO deferrable_constraints VALUES (1)');
            } catch (Throwable $t) {
                $this->connection->rollBack();

                $this->expectUniqueConstraintViolation();

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

        $this->expectUniqueConstraintViolation();
        $this->connection->commit();
    }

    public function testInsertWithNonDeferredConstraint(): void
    {
        $this->connection->beginTransaction();

        try {
            $this->connection->executeStatement('INSERT INTO deferrable_constraints VALUES (1)');
        } catch (Throwable $t) {
            $this->connection->rollBack();

            $this->expectUniqueConstraintViolation();

            throw $t;
        }
    }

    public function testCommitWithDeferredConstraintAndTransactionNesting(): void
    {
        if (! $this->connection->getDatabasePlatform()->supportsSavepoints()) {
            self::markTestSkipped('This test requires the platform to support savepoints.');
        }

        $this->skipIfDeferrableIsNotSupported();

        $this->connection->setNestTransactionsWithSavepoints(true);

        $this->connection->beginTransaction();
        $this->connection->executeStatement(sprintf('SET CONSTRAINTS "%s" DEFERRED', $this->constraintName));
        $this->connection->beginTransaction();
        $this->connection->executeStatement('INSERT INTO deferrable_constraints VALUES (1)');
        $this->connection->commit();

        $this->expectUniqueConstraintViolation();

        $this->connection->commit();
    }

    public function testCommitWithNonDeferredConstraintAndTransactionNesting(): void
    {
        if (! $this->connection->getDatabasePlatform()->supportsSavepoints()) {
            self::markTestSkipped('This test requires the platform to support savepoints.');
        }

        $this->skipIfDeferrableIsNotSupported();

        $this->connection->setNestTransactionsWithSavepoints(true);

        $this->connection->beginTransaction();
        $this->connection->beginTransaction();

        try {
            $this->connection->executeStatement('INSERT INTO deferrable_constraints VALUES (1)');
        } catch (Throwable $t) {
            $this->connection->rollBack();

            $this->expectUniqueConstraintViolation();

            throw $t;
        }
    }

    private function supportsDeferrableConstraints(): bool
    {
        $platform = $this->connection->getDatabasePlatform();

        return $platform instanceof OraclePlatform || $platform instanceof PostgreSQLPlatform;
    }

    private function skipIfDeferrableIsNotSupported(): void
    {
        if ($this->supportsDeferrableConstraints()) {
            return;
        }

        self::markTestSkipped('Only databases supporting deferrable constraints are eligible for this test.');
    }

    private function expectUniqueConstraintViolation(): void
    {
        if ($this->connection->getDatabasePlatform() instanceof SQLServerPlatform) {
            $this->expectExceptionMessage(sprintf("Violation of UNIQUE KEY constraint '%s'", $this->constraintName));

            return;
        }

        if ($this->connection->getDatabasePlatform() instanceof DB2Platform) {
            // No concrete message is provided
            $this->expectException(DriverException::class);

            return;
        }

        $this->expectException(UniqueConstraintViolationException::class);
    }

    protected function tearDown(): void
    {
        $schemaManager = $this->connection->createSchemaManager();
        $schemaManager->dropTable('deferrable_constraints');

        $this->markConnectionNotReusable();

        parent::tearDown();
    }
}
