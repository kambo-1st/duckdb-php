<?php

namespace Kambo\DuckDB;

use FFI;
use Kambo\DuckDB\Native\DuckDBFFI;
use FFI\CData;
use Kambo\DuckDB\Exception\ConnectionException;

final class Database
{
    private ?CData $databaseCData;
    private DuckDBFFI $duckDBFFI;

    /**
     * Creates a new database or opens an existing database file stored at the given path.
     * If no path is given a new in-memory database is created instead.
     *
     * @param ?string $path Path to the database file on disk, null or `:memory:` to open an in-memory database.
     * @param ?Options $options
     * @param ?DuckDBFFI $duckDBFFI
     */
    public function __construct(
        private ?string $path = null,
        private ?Options $options = null,
        ?DuckDBFFI $duckDBFFI = null
    ) {
        if ($duckDBFFI === null) {
            $duckDBFFI = DuckDBFFI::getInstance();
        }

        $this->duckDBFFI = $duckDBFFI;

        $this->open($path, $options);
    }

    /**
     * Creates a new database or opens an existing database file stored at the given path.
     * If no path is given a new in-memory database is created instead.
     * path: Path to the database file on disk, or `nullptr` or `:memory:` to open an in-memory database.
     *
     * @param ?string $path
     * @param ?Options $options
     *
     * @return void
     */
    private function open(?string $path = null, ?Options $options = null): void
    {
        $database = $this->duckDBFFI->new('duckdb_database');
        $config = null;
        if ($options !== null) {
            $config = $this->duckDBFFI->new('duckdb_config');
            $this->duckDBFFI->duckdb_create_config($this->duckDBFFI->addr($config));
            foreach ($options->toArray() as $name => $value) {
                $this->duckDBFFI->duckdb_set_config($config, $name, $value);
            }
        }

        $result = $this->duckDBFFI->duckdb_open_ext($path, $this->duckDBFFI->addr($database), $config);

        if ($result === $this->duckDBFFI->duckDBError()) {
            $this->duckDBFFI->duckdb_close($this->duckDBFFI->addr($database));

            if ($options !== null) {
                $this->duckDBFFI->duckdb_destroy_config($this->duckDBFFI->addr($config));
            }

            throw new ConnectionException('Cannot open database');
        }

        if ($options !== null) {
            $this->duckDBFFI->duckdb_destroy_config($this->duckDBFFI->addr($config));
        }

        $this->databaseCData = $database;
    }

    /**
     * Destruct object
     */
    public function __destruct()
    {
        $this->duckDBFFI->duckdb_close($this->duckDBFFI->addr($this->databaseCData));
    }
}
