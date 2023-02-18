<?php

namespace Kambo\DuckDB;

use FFI;
use FFI\CData;
use Kambo\DuckDB\Native\DuckDBFFI;
use Kambo\DuckDB\Exception\ConnectionException;
use Kambo\DuckDB\Exception\QueryException;
use Exception;

final class Connection
{
    private ?CData $connectionCData;
    private DuckDBFFI $duckDBFFI;

    /**
     * Opens a connection to a database. Connections are required to query the database, and store transactional
     * state associated with the connection.
     *
     * @param Database   $database  The database to connect to
     * @param ?DuckDBFFI $duckDBFFI
     *
     * @throws Exception
     */
    public function __construct(
        Database $database,
        ?DuckDBFFI $duckDBFFI = null
    ) {
        if ($duckDBFFI === null) {
            $duckDBFFI = DuckDBFFI::getInstance();
        }

        $this->duckDBFFI = $duckDBFFI;
        $connection = $this->duckDBFFI->new('duckdb_connection');

        $privatePropertyAccessor = function ($prop) {
            return $this->$prop;
        };
        $databaseFFI             = $privatePropertyAccessor->call($database, 'databaseCData');

        $result = $this->duckDBFFI->duckdb_connect($databaseFFI, $this->duckDBFFI->addr($connection));
        if ($result === $this->duckDBFFI->duckDBError()) {
            $this->duckDBFFI->duckdb_disconnect($this->duckDBFFI->addr($connection));

            throw new ConnectionException('Cannot connect to database');
        }

        $this->connectionCData = $connection;
    }

    /**
     * Executes a SQL query within a connection.
     *
     * @param string $query
     *
     * @return Result
     */
    public function query(string $query): Result
    {
        $queryResult = $this->duckDBFFI->new('duckdb_result');

        $result = $this->duckDBFFI->duckdb_query($this->connectionCData, $query, $this->duckDBFFI->addr($queryResult));

        if ($result === $this->duckDBFFI->DuckDBError()) {
            $error = 'Error in query: ' . $this->duckDBFFI->duckdb_result_error($this->duckDBFFI->addr($queryResult));

            $this->duckDBFFI->duckdb_destroy_result($this->duckDBFFI->addr($queryResult));
            throw new QueryException($error);
        }

        return Result::createFromCdata($this->duckDBFFI, $queryResult);
    }

    /**
     * Destruct object
     */
    public function __destruct()
    {
        $this->duckDBFFI->duckdb_disconnect($this->duckDBFFI->addr($this->connectionCData));
    }
}
