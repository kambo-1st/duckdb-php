<?php

namespace Kambo\DuckDB\Tests\Unit;

use Kambo\DuckDB\Connection;
use Kambo\DuckDB\Database;
use Kambo\DuckDB\Appender;
use Kambo\DuckDB\Exception\AppenderException;
use PHPUnit\Framework\TestCase;

class AppenderTest extends TestCase
{
    private Database $database;
    private Connection $connection;

    protected function setUp(): void
    {
        $this->database = new Database(':memory:');
        $this->connection = new Connection($this->database);
    }
    public function testAppendNonExistingTable(): void
    {
        $this->expectException(AppenderException::class);
        $this->expectExceptionMessage(
            "Failed to create appender:\nCatalog Error: Table \"main. I dont exists\" could not be found"
        );
        new Appender($this->connection, ' I dont exists');
    }

    public function testAppend(): void
    {
        $this->connection->query(
            <<<SQL
CREATE TABLE integers(
    a BOOLEAN,
    b TINYINT,
    c SMALLINT,
    d INTEGER,
    iamNull INTEGER,
    e BIGINT,
    f UTINYINT,
    g USMALLINT,
    h UINTEGER,
    i REAL,
    j DOUBLE,
    k VARCHAR
 );
SQL
        );

        $appender = new Appender($this->connection, 'integers');
        $appender->appendBool(true);
        $appender->appendInt8(127);
        $appender->appendInt16(32767);
        $appender->appendInt32(2147483647);
        $appender->appendNull();
        $appender->appendInt64(9223372036854775807);
        $appender->appendUint8(255);
        $appender->appendUint16(65535);
        $appender->appendUint32(4294967295);

        $appender->appendFloat(4294.967295);
        $appender->appendDouble(42.94967295);
        $appender->appendVarchar('Here i am');

        $appender->appenderEndRow();
        $appender->close();

        $result = $this->connection->query('SELECT * FROM integers;');

        $this->assertEquals(
            [
                [
                    'true',
                    '127',
                    '32767',
                    '2147483647',
                    null,
                    '9223372036854775807',
                    '255',
                    '65535',
                    '4294967295',
                    '4294.9673',
                    '42.94967295',
                    'Here i am',
                ],
            ],
            $result->toArray()
        );
    }
}
