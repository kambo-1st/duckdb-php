<?php

namespace Kambo\DuckDB;

use FFI\CData;
use FFI;
use Kambo\DuckDB\Native\DuckDBFFI;

final class Result
{
    public bool $success = false;

    public function __construct(
        private DuckDBFFI $duckDBFFI,
        private ?CData $CDataQueryResult = null,
    ) {
    }

    public static function createFromCdata(DuckDBFFI $duckDBFFI, CData $queryResult): Result
    {
        return new self($duckDBFFI, $queryResult);
    }

    public function columnCount(): int
    {
        return $this->duckDBFFI->duckdb_column_count($this->duckDBFFI->addr($this->CDataQueryResult));
    }
    public function rowCount(): int
    {
        return $this->duckDBFFI->duckdb_row_count($this->duckDBFFI->addr($this->CDataQueryResult));
    }

    public function toArray(): array
    {
        $rowCount    = $this->rowCount();
        $columnCount = $this->columnCount();
        $values      = [];
        for ($row = 0; $row < $rowCount; $row++) {
            for ($column = 0; $column < $columnCount; $column++) {
                $value = $this->duckDBFFI->duckdb_value_varchar(
                    $this->duckDBFFI->addr($this->CDataQueryResult),
                    $column,
                    $row
                );
                $val = null;
                if ($value !== null) {
                    $val = FFI::string($value);
                }

                $values[$row][$column] = $val;

                $this->duckDBFFI->duckdb_free($value);
            }
        }

        return $values;
    }

    /**
     * Destruct object
     */
    public function __destruct()
    {
        $this->duckDBFFI->duckdb_destroy_result($this->duckDBFFI->addr($this->CDataQueryResult));
    }
}
