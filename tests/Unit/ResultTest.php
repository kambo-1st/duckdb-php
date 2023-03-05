<?php

namespace Kambo\DuckDB\Tests\Unit;

use Kambo\DuckDB\Connection;
use Kambo\DuckDB\Database;
use PHPUnit\Framework\TestCase;
use Kambo\DuckDB\Type;

final class ResultTest extends TestCase
{
    private Database $db;
    private Connection $conn;

    protected function setUp(): void
    {
        $this->db = new Database(':memory:');
        $this->conn = new Connection($this->db);
    }

    public function testColumnCount()
    {
        $result = $this->conn->query('SELECT 1 AS col, 2 as col2');

        $this->assertEquals(2, $result->columnCount());
    }

    public function testRowCount()
    {
        $this->conn->query('CREATE TABLE integers(i INTEGER, j INTEGER);');
        $this->conn->query('INSERT INTO integers VALUES (3,4), (5,6), (7, NULL) ');

        $result = $this->conn->query('SELECT * FROM integers;');

        $this->assertEquals(3, $result->rowCount());
    }

    public function testGetColumnType()
    {
        $this->conn->query('CREATE TABLE integers(i INTEGER, j INTEGER);');
        $this->conn->query('INSERT INTO integers VALUES (3,4), (5,6), (7, NULL) ');

        $result = $this->conn->query('SELECT * FROM integers;');

        $this->assertSame(Type::INTEGER, $result->getColumnType(0));
    }

    public function testToArray()
    {
        $this->conn->query('CREATE TABLE integers(i INTEGER, j INTEGER);');
        $this->conn->query('INSERT INTO integers VALUES (3,4), (5,6), (7, NULL) ');

        $result = $this->conn->query('SELECT * FROM integers;');

        $this->assertSame(
            [
                [
                    3,
                    4,
                ],
                [
                    5,
                    6,
                ],
                [
                    7,
                    null,
                ],
            ],
            $result->toArray()
        );
    }

    protected function tearDown(): void
    {
        unset($this->conn);
        unset($this->db);
    }
}
