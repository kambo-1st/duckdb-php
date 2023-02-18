<?php

namespace Kambo\DuckDB\Tests\Unit;

use Kambo\DuckDB\Connection;
use Kambo\DuckDB\Database;
use PHPUnit\Framework\TestCase;
use Kambo\DuckDB\Exception\QueryException;

class ConnectionTest extends TestCase
{
    private Database $database;
    private Connection $connection;

    protected function setUp(): void
    {
        $this->database = new Database(':memory:');
        $this->connection = new Connection($this->database);
    }

    public function testQueryWithValidQuery()
    {
        $result = $this->connection->query('SELECT 1 AS col');
        $this->assertIsObject($result);
    }

    public function testQueryWithInvalidQuery()
    {
        $this->expectException(QueryException::class);
        $this->expectExceptionMessage('Error in query: Parser Error: SELECT clause without selection list');
        $this->connection->query('SELECT FROM invalid_table');
    }

    public function testToArray(): void
    {
        $this->connection->query('CREATE TABLE integers(i INTEGER, j INTEGER);');
        $this->connection->query('INSERT INTO integers VALUES (3,4), (5,6), (7, NULL) ');

        $result = $this->connection->query('SELECT * FROM integers;');

        $this->assertEquals(
            [
                [
                    '3',
                    '4',
                ],
                [
                    '5',
                    '6',
                ],
                [
                    '7',
                    null,
                ],
            ],
            $result->toArray()
        );
    }
}
