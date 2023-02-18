<?php

namespace Kambo\DuckDB\Native;

use FFI;
use FFI\CData;
use Kambo\DuckDB\Exception\MissingLibraryException;
use Kambo\DuckDB\Exception\NotImplementedException;

use function file_get_contents;

/**
 * Thin wrapper around DuckDB API, represents low level API for duck DB.
 */
final class DuckDBFFI
{
    private static ?DuckDBFFI $instance = null;

    public function __construct(private FFI $fii)
    {
    }

    /**
     * @return static
     */
    public static function getInstance(): self
    {
        if (self::$instance == null) {
            self::$instance = self::create() ;
        }

        return self::$instance;
    }

    /**
     * @param LocateDuckDB|null $locator
     *
     * @return static
     */
    public static function create(?LocateDuckDB $locator = null): self
    {
        if ($locator === null) {
            $locator = new Locator();
        }

        $path = $locator->getLibraryPath();

        return self::createWithLibraryInPath($path);
    }

    /**
     * @param string $path
     *
     * @return static
     */
    public static function createWithLibraryInPath(string $path): self
    {
        $duckDBFFI = FFI::cdef(file_get_contents(__DIR__ . '/duckdb-ffi.h'), $path);

        return new self($duckDBFFI);
    }

    /**
     * Method that creates an arbitrary C structure.
     *
     * @param string $type
     *
     * @return CData|null
     */
    public function new(string $type): ?CData
    {
        return $this->fii->new($type);
    }

    /**
     * Returns C pointer to the given C data structure. The pointer is
     * not "owned" and won't be free. Anyway, this is a potentially
     * unsafe operation, because the life-time of the returned pointer
     * may be longer than life-time of the source object, and this may
     * cause dangling pointer dereference (like in regular C).
     *
     * @param CData $ptr
     *
     * @return CData
     */
    public function addr(CData $ptr): CData
    {
        return FFI::addr($ptr);
    }

    /**
     * Opens a connection to a database. Connections are required to query the database, and store transactional state
     * associated with the connection.
     *
     * @param CData $database The database file to connect to.
     * @param CData $connection The result connection object.
     *
     * @return int `DuckDBSuccess` on success or `DuckDBError` on failure.
     */
    public function duckdb_connect(CData $database, CData $connection): int
    {
        return $this->fii->duckdb_connect($database, $connection);
    }

    /**
     * Creates a new database or opens an existing database file stored at the given path.
     * If no path is given, a new in-memory database is created instead.
     *
     * @param ?string $path Path to the database file on disk, or `null` or `:memory:` to open an in-memory database.
     * @param CData $database The result database object.
     *
     * @return int `DuckDBSuccess` on success or `DuckDBError` on failure.
     */
    public function duckdb_open(?string $path, CData $database): int
    {
        return $this->fii->duckdb_open($path, $database);
    }

    /**
     * Extended version of duckdb_open. Creates a new database or opens an existing database file stored
     * at the given path.
     *
     * @param ?string $path Path to the database file on disk, or `null` or `:memory:` to open an in-memory database.
     * @param CData $database The result database object.
     * @param ?CData $config (Optional) configuration used to start up the database system.
     *
     * @return int `DuckDBSuccess` on success or `DuckDBError` on failure.
     */
    public function duckdb_open_ext(?string $path, CData $database, ?CData $config = null): int
    {
        return $this->fii->duckdb_open_ext($path, $database, $config, null);
    }

    public function duckDBError()
    {
        return $this->fii->DuckDBError;
    }

    /**
     * Executes a SQL query within a connection and stores the full (materialized) result in the out_result pointer.
     * If the query fails to execute, DuckDBError is returned and the error message can be retrieved by calling
     * `duckdb_result_error`.
     *
     * Note that after running `duckdb_query`, `duckdb_destroy_result` must be called on the result object even if the
     * query fails, otherwise the error stored within the result will not be freed correctly.
     *
     * @param CData $connection The connection to perform the query in.
     * @param string $query The SQL query to run.
     * @param CData $addr The query result.
     *
     * @return int `DuckDBSuccess` on success or `DuckDBError` on failure.
     */
    public function duckdb_query(CData $connection, string $query, CData $addr): int
    {
        return $this->fii->duckdb_query($connection, $query, $addr);
    }

    /**
     * Returns the number of rows present in the result object.
     *
     * @param CData $result The result object.
     *
     * @return int The number of rows present in the result object.
     */
    public function duckdb_row_count(CData $result): int
    {
        return $this->fii->duckdb_row_count($result);
    }

    /**
     * Returns the number of columns present in the result object.
     *
     * @param CData $result The result object.
     *
     * @return int The number of columns present in the result object.
     */
    public function duckdb_column_count(CData $result): int
    {
        return $this->fii->duckdb_column_count($result);
    }

    /**
     * DEPRECATED: use duckdb_value_string instead.
     * This function does not work correctly if the string contains null bytes.
     *
     * @param CData $result The result object.
     * @param int $column The column index.
     * @param int $row The row index.
     *
     * @return string|null The text value at the specified location as a null-terminated string, or null if the
     * value cannot be converted. The result must be freed with `duckdb_free`.
     * @deprecated
     */
    public function duckdb_value_varchar(CData $addr, int $column, int $row): mixed
    {
        return $this->fii->duckdb_value_varchar($addr, $column, $row);
    }

    /**
     * Free a value returned from `duckdb_malloc`, `duckdb_value_varchar` or `duckdb_value_blob`.
     *
     * @param ?CData $ptr The memory region to de-allocate.
     */
    public function duckdb_free(?CData $ptr)
    {
        return $this->fii->duckdb_free($ptr);
    }

    /**
     * Closes the specified database and de-allocates all memory allocated for that database.
     * This should be called after you are done with any database allocated through `duckdb_open`.
     * Note that failing to call `duckdb_close` (in case of e.g. a program crash) will not cause data corruption.
     * Still, it is recommended to always correctly close a database object after you are done with it.
     *
     * @param CData $database The database object to shut down.
     *
     * @return void
     */
    public function duckdb_close(CData $database): void
    {
        $this->fii->duckdb_close($database);
    }

    /**
     * Closes the specified connection and de-allocates all memory allocated for that connection.
     *
     * @param CData $connection The connection to close.
     *
     * @return void
     */
    public function duckdb_disconnect(CData $connection): void
    {
        $this->fii->duckdb_disconnect($connection);
    }

    /**
     * Returns the error message contained within the result. The error is only set
     * if `duckdb_query` returns `DuckDBError`. The result of this function must not be freed.
     * It will be cleaned up when `duckdb_destroy_result` is called.
     *
     * @param CData $result The result object to fetch the error from.
     *
     * @return string The error of the result.
     */
    public function duckdb_result_error(CData $result): string
    {
        return $this->fii->duckdb_result_error($result);
    }

    /**
     * Closes the result and de-allocates all memory allocated for that connection.
     *
     * @param CData $result The result to destroy.
     */
    public function duckdb_destroy_result(CData $result): void
    {
        $this->fii->duckdb_destroy_result($result);
    }

    /**
     * Initializes an empty configuration object that can be used to provide start-up options for the DuckDB instance
     * through `duckdb_open_ext`.
     * This will always succeed unless there is a malloc failure.
     *
     * @param CData $config The result configuration object.
     *
     * @return int `DuckDBSuccess` on success or `DuckDBError` on failure.
     */
    public function duckdb_create_config(CData $config): int
    {
        return $this->fii->duckdb_create_config($config);
    }

    /**
     * Sets the specified option for the specified configuration. The configuration option is indicated by name.
     * To obtain a list of config options, see `duckdb_get_config_flag`.
     * In the source code, configuration options are defined in `config.cpp`.
     * This can fail if either the name is invalid, or if the value provided for the option is invalid.
     *
     * @param CData $config The configuration object to set the option on.
     * @param string $name The name of the configuration flag to set.
     * @param string $option The value to set the configuration flag to.
     *
     * @return int `DuckDBSuccess` on success or `DuckDBError` on failure.
     */
    public function duckdb_set_config(CData $config, string $name, string $option): int
    {
        return $this->fii->duckdb_set_config($config, $name, $option);
    }

    /**
     * Destroys the specified configuration option and de-allocates all memory allocated for the object.
     *
     * @param CData $config The configuration object to destroy.
     */
    public function duckdb_destroy_config(CData $config): void
    {
        $this->fii->duckdb_destroy_config($config);
    }
}
