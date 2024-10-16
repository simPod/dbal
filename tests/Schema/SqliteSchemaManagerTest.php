<?php

namespace Doctrine\DBAL\Tests\Schema;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Platforms\SqlitePlatform;
use Doctrine\DBAL\Result;
use Doctrine\DBAL\Schema\SqliteSchemaManager;
use PHPUnit\Framework\TestCase;
use ReflectionMethod;

class SqliteSchemaManagerTest extends TestCase
{
    /** @dataProvider getDataColumnCollation */
    public function testParseColumnCollation(?string $collation, string $column, string $sql): void
    {
        $conn = $this->createMock(Connection::class);

        $manager = new SqliteSchemaManager($conn, new SqlitePlatform());
        $ref     = new ReflectionMethod($manager, 'parseColumnCollationFromSQL');
        $ref->setAccessible(true);

        self::assertSame($collation, $ref->invoke($manager, $column, $sql));
    }

    /** @return mixed[][] */
    public static function getDataColumnCollation(): iterable
    {
        return [
            [
                'RTRIM',
                'a',
                'CREATE TABLE "a" ("a" text DEFAULT "aa" COLLATE "RTRIM" NOT NULL)',
            ],
            [
                'utf-8',
                'a',
                'CREATE TABLE "a" ("b" text UNIQUE NOT NULL COLLATE NOCASE, '
                    . '"a" text DEFAULT "aa" COLLATE "utf-8" NOT NULL)',
            ],
            [
                'NOCASE',
                'a',
                'CREATE TABLE "a" ("a" text DEFAULT (lower(ltrim(" a") || rtrim("a ")))'
                    . ' CHECK ("a") NOT NULL COLLATE NOCASE UNIQUE, "b" text COLLATE RTRIM)',
            ],
            [
                null,
                'a',
                'CREATE TABLE "a" ("a" text CHECK ("a") NOT NULL, "b" text COLLATE RTRIM)',
            ],
            [
                'RTRIM',
                'a"b',
                'CREATE TABLE "a" ("a""b" text COLLATE RTRIM)',
            ],
            [
                'BINARY',
                'b',
                'CREATE TABLE "a" (bb TEXT COLLATE RTRIM, b VARCHAR(42) NOT NULL COLLATE BINARY)',
            ],
            [
                'BINARY',
                'b',
                'CREATE TABLE "a" (bbb TEXT COLLATE NOCASE, bb TEXT COLLATE RTRIM, '
                    . 'b VARCHAR(42) NOT NULL COLLATE BINARY)',
            ],
            [
                'BINARY',
                'b',
                'CREATE TABLE "a" (b VARCHAR(42) NOT NULL COLLATE BINARY, bb TEXT COLLATE RTRIM)',
            ],
            [
                'utf-8',
                'bar#',
                'CREATE TABLE dummy_table (id INTEGER NOT NULL, foo VARCHAR(255) COLLATE "utf-8" NOT NULL, '
                    . '"bar#" VARCHAR(255) COLLATE "utf-8" NOT NULL, baz VARCHAR(255) COLLATE "utf-8" NOT NULL, '
                    . 'PRIMARY KEY(id))',
            ],
            [
                null,
                'bar#',
                'CREATE TABLE dummy_table (id INTEGER NOT NULL, foo VARCHAR(255) NOT NULL,'
                    . ' "bar#" VARCHAR(255) NOT NULL, baz VARCHAR(255) NOT NULL, PRIMARY KEY(id))',
            ],
            [
                'utf-8',
                'baz',
                'CREATE TABLE dummy_table (id INTEGER NOT NULL, foo VARCHAR(255) COLLATE "utf-8" NOT NULL,'
                    . ' "bar#" INTEGER NOT NULL, baz VARCHAR(255) COLLATE "utf-8" NOT NULL, PRIMARY KEY(id))',
            ],
            [
                null,
                'baz',
                'CREATE TABLE dummy_table (id INTEGER NOT NULL, foo VARCHAR(255) NOT NULL, "bar#" INTEGER NOT NULL, '
                    . 'baz VARCHAR(255) NOT NULL, PRIMARY KEY(id))',
            ],
            [
                'utf-8',
                'bar/',
                'CREATE TABLE dummy_table (id INTEGER NOT NULL, foo VARCHAR(255) COLLATE "utf-8" NOT NULL, '
                    . '"bar/" VARCHAR(255) COLLATE "utf-8" NOT NULL, baz VARCHAR(255) COLLATE "utf-8" NOT NULL,'
                    . ' PRIMARY KEY(id))',
            ],
            [
                null,
                'bar/',
                'CREATE TABLE dummy_table (id INTEGER NOT NULL, foo VARCHAR(255) NOT NULL, '
                    . '"bar/" VARCHAR(255) NOT NULL, baz VARCHAR(255) NOT NULL, PRIMARY KEY(id))',
            ],
            [
                'utf-8',
                'baz',
                'CREATE TABLE dummy_table (id INTEGER NOT NULL, foo VARCHAR(255) COLLATE "utf-8" NOT NULL, '
                    . '"bar/" INTEGER NOT NULL, baz VARCHAR(255) COLLATE "utf-8" NOT NULL, PRIMARY KEY(id))',
            ],
            [
                null,
                'baz',
                'CREATE TABLE dummy_table (id INTEGER NOT NULL, foo VARCHAR(255) NOT NULL,'
                    . ' "bar/" INTEGER NOT NULL, baz VARCHAR(255) NOT NULL, PRIMARY KEY(id))',
            ],
        ];
    }

    /** @dataProvider getDataColumnComment */
    public function testParseColumnCommentFromSQL(?string $comment, string $column, string $sql): void
    {
        $conn = $this->createMock(Connection::class);

        $manager = new SqliteSchemaManager($conn, new SqlitePlatform());
        $ref     = new ReflectionMethod($manager, 'parseColumnCommentFromSQL');
        $ref->setAccessible(true);

        self::assertSame($comment, $ref->invoke($manager, $column, $sql));
    }

    /** @return mixed[][] */
    public static function getDataColumnComment(): iterable
    {
        return [
            'Single column with no comment' => [
                null,
                'a',
                'CREATE TABLE "a" ("a" TEXT DEFAULT "a" COLLATE RTRIM)',
            ],
            'Single column with type comment' => [
                '(DC2Type:x)',
                'a',
                'CREATE TABLE "a" ("a" CLOB DEFAULT NULL COLLATE BINARY --(DC2Type:x)
)',
            ],
            'Multiple similar columns with type comment 1' => [
                null,
                'b',
                'CREATE TABLE "a" (a TEXT COLLATE RTRIM, "b" TEXT DEFAULT "a" COLLATE RTRIM, '
                    . '"bb" CLOB DEFAULT NULL COLLATE BINARY --(DC2Type:x)
)',
            ],
            'Multiple similar columns with type comment 2' => [
                '(DC2Type:x)',
                'b',
                'CREATE TABLE "a" (a TEXT COLLATE RTRIM, "bb" TEXT DEFAULT "a" COLLATE RTRIM, '
                    . '"b" CLOB DEFAULT NULL COLLATE BINARY --(DC2Type:x)
)',
            ],
            'Multiple similar columns on different lines, with type comment 1' => [
                null,
                'bb',
                'CREATE TABLE "a" (a TEXT COLLATE RTRIM, "b" CLOB DEFAULT NULL COLLATE BINARY --(DC2Type:x)
, "bb" TEXT DEFAULT "a" COLLATE RTRIM',
            ],
            'Multiple similar columns on different lines, with type comment 2' => [
                '(DC2Type:x)',
                'bb',
                'CREATE TABLE "a" (a TEXT COLLATE RTRIM, "bb" CLOB DEFAULT NULL COLLATE BINARY --(DC2Type:x)
, "b" TEXT DEFAULT "a" COLLATE RTRIM',
            ],
            'Column with numeric but no comment 1' => [
                null,
                'a',
                'CREATE TABLE "a" ("a" NUMERIC(10, 0) NOT NULL, "b" CLOB NOT NULL --(DC2Type:array)
, "c" CHAR(36) NOT NULL --(DC2Type:guid)
)',
            ],
            'Column with numeric but no comment 2' => [
                null,
                'a',
                'CREATE TABLE "b" ("a" NUMERIC(10, 0) NOT NULL, "b" CLOB NOT NULL --(DC2Type:array)
, "c" CHAR(36) NOT NULL --(DC2Type:guid)
)',
            ],
            'Column with numeric but no comment 3' => [
                '(DC2Type:guid)',
                'c',
                'CREATE TABLE "b" ("a" NUMERIC(10, 0) NOT NULL, "b" CLOB NOT NULL --(DC2Type:array)
, "c" CHAR(36) NOT NULL --(DC2Type:guid)
)',
            ],
            'Column with numeric but no comment 4' => [
                '(DC2Type:array)',
                'b',
                'CREATE TABLE "b" ("a" NUMERIC(10, 0) NOT NULL,
                    "b" CLOB NOT NULL, --(DC2Type:array)
                    "c" CHAR(36) NOT NULL --(DC2Type:guid)
                )',
            ],
            'Column "bar", select "bar" with no comment' => [
                null,
                'bar',
                'CREATE TABLE dummy_table (
                    id INTEGER NOT NULL,
                    foo VARCHAR(255) COLLATE "utf-8" NOT NULL,
                    "bar" VARCHAR(255) COLLATE "utf-8" NOT NULL,
                    baz VARCHAR(255) COLLATE "utf-8" NOT NULL,
                    PRIMARY KEY(id)
                )',
            ],
            'Column "bar", select "bar" with type comment' => [
                '(DC2Type:x)',
                'bar',
                'CREATE TABLE dummy_table (
                    id INTEGER NOT NULL,
                    foo VARCHAR(255) COLLATE "utf-8" NOT NULL,
                    "bar" VARCHAR(255) COLLATE "utf-8" NOT NULL, --(DC2Type:x)
                    baz VARCHAR(255) COLLATE "utf-8" NOT NULL, --(DC2Type:y)
                    PRIMARY KEY(id)
                )',
            ],
            'Column "bar", select "baz" with no comment' => [
                null,
                'baz',
                'CREATE TABLE dummy_table (
                    id INTEGER NOT NULL,
                    foo VARCHAR(255) COLLATE "utf-8" NOT NULL,
                    "bar" INTEGER NOT NULL,
                    baz VARCHAR(255) COLLATE "utf-8" NOT NULL,
                    PRIMARY KEY(id)
                )',
            ],
            'Column "bar", select "baz" with type comment' => [
                '(DC2Type:y)',
                'baz',
                'CREATE TABLE dummy_table (
                    id INTEGER NOT NULL,
                    foo VARCHAR(255) COLLATE "utf-8" NOT NULL,
                    "bar" INTEGER NOT NULL, --(DC2Type:x)
                    baz VARCHAR(255) COLLATE "utf-8" NOT NULL, --(DC2Type:y)
                    PRIMARY KEY(id)
                )',
            ],

            'Column "bar#", select "bar#" with no comment' => [
                null,
                'bar#',
                'CREATE TABLE dummy_table (
                    id INTEGER NOT NULL,
                    foo VARCHAR(255) COLLATE "utf-8" NOT NULL,
                    "bar#" VARCHAR(255) COLLATE "utf-8" NOT NULL,
                    baz VARCHAR(255) COLLATE "utf-8" NOT NULL,
                    PRIMARY KEY(id)
                )',
            ],
            'Column "bar#", select "bar#" with type comment' => [
                '(DC2Type:x)',
                'bar#',
                'CREATE TABLE dummy_table (
                    id INTEGER NOT NULL,
                    foo VARCHAR(255) COLLATE "utf-8" NOT NULL,
                    "bar#" VARCHAR(255) COLLATE "utf-8" NOT NULL, --(DC2Type:x)
                    baz VARCHAR(255) COLLATE "utf-8" NOT NULL, --(DC2Type:y)
                    PRIMARY KEY(id)
                )',
            ],
            'Column "bar#", select "baz" with no comment' => [
                null,
                'baz',
                'CREATE TABLE dummy_table (
                    id INTEGER NOT NULL,
                    foo VARCHAR(255) COLLATE "utf-8" NOT NULL,
                    "bar#" INTEGER NOT NULL,
                    baz VARCHAR(255) COLLATE "utf-8" NOT NULL,
                    PRIMARY KEY(id)
                )',
            ],
            'Column "bar#", select "baz" with type comment' => [
                '(DC2Type:y)',
                'baz',
                'CREATE TABLE dummy_table (
                    id INTEGER NOT NULL,
                    foo VARCHAR(255) COLLATE "utf-8" NOT NULL,
                    "bar#" INTEGER NOT NULL, --(DC2Type:x)
                    baz VARCHAR(255) COLLATE "utf-8" NOT NULL, --(DC2Type:y)
                    PRIMARY KEY(id)
                )',
            ],

            'Column "bar/", select "bar/" with no comment' => [
                null,
                'bar/',
                'CREATE TABLE dummy_table (
                    id INTEGER NOT NULL,
                    foo VARCHAR(255) COLLATE "utf-8" NOT NULL,
                    "bar/" VARCHAR(255) COLLATE "utf-8" NOT NULL,
                    baz VARCHAR(255) COLLATE "utf-8" NOT NULL,
                    PRIMARY KEY(id)
                    )',
            ],
            'Column "bar/", select "bar/" with type comment' => [
                '(DC2Type:x)',
                'bar/',
                'CREATE TABLE dummy_table (
                    id INTEGER NOT NULL,
                    foo VARCHAR(255) COLLATE "utf-8" NOT NULL,
                    "bar/" VARCHAR(255) COLLATE "utf-8" NOT NULL, --(DC2Type:x)
                    baz VARCHAR(255) COLLATE "utf-8" NOT NULL, --(DC2Type:y)
                    PRIMARY KEY(id)
                )',
            ],
            'Column "bar/", select "baz" with no comment' => [
                null,
                'baz',
                'CREATE TABLE dummy_table (
                    id INTEGER NOT NULL,
                    foo VARCHAR(255) COLLATE "utf-8" NOT NULL,
                    "bar/" INTEGER NOT NULL,
                    baz VARCHAR(255) COLLATE "utf-8" NOT NULL,
                    PRIMARY KEY(id)
                )',
            ],
            'Column "bar/", select "baz" with type comment' => [
                '(DC2Type:y)',
                'baz',
                'CREATE TABLE dummy_table (
                    id INTEGER NOT NULL,
                    foo VARCHAR(255) COLLATE "utf-8" NOT NULL,
                    "bar/" INTEGER COLLATE "utf-8" NOT NULL, --(DC2Type:x)
                    baz VARCHAR(255) COLLATE "utf-8" NOT NULL, --(DC2Type:y)
                    PRIMARY KEY(id)
                )',
            ],
        ];
    }

    /**
     * TODO move to functional test once SqliteSchemaManager::selectForeignKeyColumns can honor database/schema name
     * https://github.com/doctrine/dbal/blob/3.8.3/src/Schema/SqliteSchemaManager.php#L740
     */
    public function testListTableForeignKeysDefaultDatabasePassing(): void
    {
        $conn = $this->createMock(Connection::class);

        $manager = new class ($conn, new SqlitePlatform()) extends SqliteSchemaManager {
            public static string $passedDatabaseName;

            protected function selectForeignKeyColumns(string $databaseName, ?string $tableName = null): Result
            {
                self::$passedDatabaseName = $databaseName;

                return parent::selectForeignKeyColumns($databaseName, $tableName);
            }
        };

        $manager->listTableForeignKeys('t');
        self::assertSame('main', $manager::$passedDatabaseName);

        $manager->listTableForeignKeys('t', 'd');
        self::assertSame('d', $manager::$passedDatabaseName);

        $manager->listTableForeignKeys('t');
        self::assertSame('main', $manager::$passedDatabaseName);
    }
}
