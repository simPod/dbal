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

final class UniqueConstraintViolationsTest extends FunctionalTestCase
{
    private string $constraintName = '';

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

        $table = new Table('unique_constraint_violations');
        $table->addColumn('unique_field', 'integer', ['notnull' => true]);
        $schemaManager->createTable($table);

        if ($platform instanceof OraclePlatform) {
            $createConstraint = sprintf(
                'ALTER TABLE unique_constraint_violations ' .
                'ADD CONSTRAINT %s UNIQUE (unique_field) DEFERRABLE INITIALLY IMMEDIATE',
                $constraintName,
            );
        } elseif ($platform instanceof PostgreSQLPlatform) {
            $createConstraint = sprintf(
                'ALTER TABLE unique_constraint_violations ' .
                'ADD CONSTRAINT %s UNIQUE (unique_field) DEFERRABLE INITIALLY IMMEDIATE',
                $constraintName,
            );
        } elseif ($platform instanceof SqlitePlatform) {
            $createConstraint = sprintf(
                'CREATE UNIQUE INDEX %s ON unique_constraint_violations(unique_field)',
                $constraintName,
            );
        } else {
            $createConstraint = new UniqueConstraint($constraintName, ['unique_field']);
        }

        if ($createConstraint instanceof UniqueConstraint) {
            $schemaManager->createUniqueConstraint($createConstraint, 'unique_constraint_violations');
        } else {
            $this->connection->executeStatement($createConstraint);
        }

        $this->connection->executeStatement('INSERT INTO unique_constraint_violations VALUES (1)');
    }

    public function testTransactionalViolatesDeferredConstraint(): void
    {
        $this->skipIfDeferrableIsNotSupported();

        try {
            $this->connection->transactional(function (Connection $connection): void {
                $connection->executeStatement(sprintf('SET CONSTRAINTS "%s" DEFERRED', $this->constraintName));

                $connection->executeStatement('INSERT INTO unique_constraint_violations VALUES (1)');
            });
        } catch (Throwable $throwable) {
            $this->expectUniqueConstraintViolation($throwable);
        }
    }

    public function testTransactionalViolatesConstraint(): void
    {
        $this->connection->transactional(function (Connection $connection): void {
            try {
                $connection->executeStatement('INSERT INTO unique_constraint_violations VALUES (1)');
            } catch (Throwable $throwable) {
                $this->expectUniqueConstraintViolation($throwable);
            }
        });
    }

    public function testTransactionalViolatesDeferredConstraintWhileUsingTransactionNesting(): void
    {
        if (! $this->connection->getDatabasePlatform()->supportsSavepoints()) {
            self::markTestSkipped('This test requires the platform to support savepoints.');
        }

        $this->skipIfDeferrableIsNotSupported();

        $this->connection->setNestTransactionsWithSavepoints(true);

        try {
            $this->connection->transactional(function (Connection $connection): void {
                $connection->executeStatement(sprintf('SET CONSTRAINTS "%s" DEFERRED', $this->constraintName));
                $connection->beginTransaction();
                $connection->executeStatement('INSERT INTO unique_constraint_violations VALUES (1)');
                $connection->commit();
            });
        } catch (Throwable $throwable) {
            $this->expectUniqueConstraintViolation($throwable);
        }
    }

    public function testTransactionalViolatesConstraintWhileUsingTransactionNesting(): void
    {
        if (! $this->connection->getDatabasePlatform()->supportsSavepoints()) {
            self::markTestSkipped('This test requires the platform to support savepoints.');
        }

        $this->connection->setNestTransactionsWithSavepoints(true);

        $this->connection->transactional(function (Connection $connection): void {
            $connection->beginTransaction();

            try {
                $this->connection->executeStatement('INSERT INTO unique_constraint_violations VALUES (1)');
            } catch (Throwable $throwable) {
                $this->connection->rollBack();

                $this->expectUniqueConstraintViolation($throwable);
            }
        });
    }

    public function testCommitViolatesDeferredConstraint(): void
    {
        $this->skipIfDeferrableIsNotSupported();

        $this->connection->beginTransaction();
        $this->connection->executeStatement(sprintf('SET CONSTRAINTS "%s" DEFERRED', $this->constraintName));
        $this->connection->executeStatement('INSERT INTO unique_constraint_violations VALUES (1)');

        try {
            $this->connection->commit();
        } catch (Throwable $throwable) {
            $this->expectUniqueConstraintViolation($throwable);
        }
    }

    public function testInsertViolatesConstraint(): void
    {
        $this->connection->beginTransaction();

        try {
            $this->connection->executeStatement('INSERT INTO unique_constraint_violations VALUES (1)');
        } catch (Throwable $t) {
            $this->connection->rollBack();

            $this->expectUniqueConstraintViolation($t);
        }
    }

    public function testCommitViolatesDeferredConstraintWhileUsingTransactionNesting(): void
    {
        if (! $this->connection->getDatabasePlatform()->supportsSavepoints()) {
            self::markTestSkipped('This test requires the platform to support savepoints.');
        }

        $this->skipIfDeferrableIsNotSupported();

        $this->connection->setNestTransactionsWithSavepoints(true);

        $this->connection->beginTransaction();
        $this->connection->executeStatement(sprintf('SET CONSTRAINTS "%s" DEFERRED', $this->constraintName));
        $this->connection->beginTransaction();
        $this->connection->executeStatement('INSERT INTO unique_constraint_violations VALUES (1)');
        $this->connection->commit();

        try {
                $this->connection->commit();
        } catch (Throwable $throwable) {
            $this->expectUniqueConstraintViolation($throwable);
        }
    }

    public function testCommitViolatesConstraintWhileUsingTransactionNesting(): void
    {
        if (! $this->connection->getDatabasePlatform()->supportsSavepoints()) {
            self::markTestSkipped('This test requires the platform to support savepoints.');
        }

        $this->skipIfDeferrableIsNotSupported();

        $this->connection->setNestTransactionsWithSavepoints(true);

        $this->connection->beginTransaction();
        $this->connection->beginTransaction();

        try {
            $this->connection->executeStatement('INSERT INTO unique_constraint_violations VALUES (1)');
        } catch (Throwable $t) {
            $this->connection->rollBack();

            $this->expectUniqueConstraintViolation($t);
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

    private function expectUniqueConstraintViolation(Throwable $throwable): void
    {
        if ($this->connection->getDatabasePlatform() instanceof SQLServerPlatform) {
            self::assertStringContainsString(
                sprintf("Violation of UNIQUE KEY constraint '%s'", $this->constraintName),
                $throwable->getMessage()
            );

            return;
        }

        if ($this->connection->getDatabasePlatform() instanceof DB2Platform) {
            // No concrete message is provided
            self::assertInstanceOf(DriverException::class, $throwable);

            return;
        }

        if ($this->connection->getDatabasePlatform() instanceof OraclePlatform) {
            self::assertInstanceOf(DriverException::class, $throwable);
            $previous = $throwable->getPrevious();
            $this->assertNotNull($previous);
            self::assertInstanceOf(UniqueConstraintViolationException::class, $previous);

            return;
        }

        self::assertInstanceOf(UniqueConstraintViolationException::class, $throwable);
    }

    protected function tearDown(): void
    {
        $schemaManager = $this->connection->createSchemaManager();
        $schemaManager->dropTable('unique_constraint_violations');

        $this->markConnectionNotReusable();

        parent::tearDown();
    }
}
