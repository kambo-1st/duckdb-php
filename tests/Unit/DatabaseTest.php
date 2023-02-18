<?php

namespace Kambo\DuckDB\Tests\Unit;

use Kambo\DuckDB\Connection;
use Kambo\DuckDB\Options;
use PHPUnit\Framework\TestCase;
use Kambo\DuckDB\Database;
use Kambo\DuckDB\Exception\ConnectionException;

use function sys_get_temp_dir;
use function uniqid;
use function is_dir;
use function mkdir;

use const DIRECTORY_SEPARATOR;

class DatabaseTest extends TestCase
{
    public function testOpenInMemory(): void
    {
        $db = new Database(':memory:');
        $this->assertInstanceOf(Database::class, $db);
    }

    public function testOpenWithValidPath(): void
    {
        $db = new Database($this->createTempFolder('duckdb-ut') . '/test.db');
        $this->assertInstanceOf(Database::class, $db);
    }

    public function testOpenWithInvalidPath(): void
    {
        $this->expectException(ConnectionException::class);
        new Database('/path/to/nowhere');
    }

    public function testSetOptions(): void
    {
        $options = new Options();
        $options->set('default_order', 'desc');

        $database = new Database(':memory:', $options);
        $connection = new Connection($database);

        $result = $connection->query("SELECT current_setting('default_order');");

        $this->assertEquals([['desc']], $result->toArray());
    }

    private function createTempFolder($prefix = 'temp', $mode = 0777)
    {
        $folder = sys_get_temp_dir() . DIRECTORY_SEPARATOR . $prefix . uniqid();

        if (!is_dir($folder)) {
            mkdir($folder, $mode, true);
        }

        return $folder;
    }
}
