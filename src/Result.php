<?php

namespace Kambo\DuckDB;

use FFI\CData;
use FFI;
use Kambo\DuckDB\Native\DuckDBFFI;
use Kambo\DuckDB\Exception\DuckDBException;

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

    /**
     * Converts the result to an array.
     *
     * @return array
     */
    public function toArray(): array
    {
        $rowCount    = $this->rowCount();
        $columnCount = $this->columnCount();
        $values      = [];
        for ($row = 0; $row < $rowCount; $row++) {
            for ($column = 0; $column < $columnCount; $column++) {
                $values[$row][$column] = $this->getValue($column, $row);
            }
        }

        return $values;
    }

    /**
     * Get the column type
     *
     * @param int $column
     *
     * @return Type|null
     */
    public function getColumnType(int $column): ?Type
    {
        $columnType = $this->duckDBFFI->duckdb_column_type($this->duckDBFFI->addr($this->CDataQueryResult), $column);

        return Type::from($columnType);
    }

    /**
     * Get the column name
     * 
     * @return array
     */
    public function getColumns(): array
    {
        $columnCount = $this->columnCount();
        $values      = [];
        for ($column = 0; $column < $columnCount; $column++) {
            $values[$column] = $this->duckDBFFI->duckdb_column_name($this->duckDBFFI->addr($this->CDataQueryResult), $column);
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

    /**
     * @param int $column
     * @param int $row
     *
     * @return mixed
     */
    private function getValue(int $column, int $row): mixed
    {
        $type = $this->getColumnType($column);
        return match ($type) {
            Type::VARCHAR   => $this->getValString($column, $row),
            Type::INTEGER   => $this->getValInteger32($column, $row),
            Type::UINTEGER  => $this->getValUinteger32($column, $row),
            Type::BOOLEAN   => $this->getValueBoolean($column, $row),
            Type::TINYINT   => $this->getValInteger8($column, $row),
            Type::UTINYINT  => $this->getValUinteger8($column, $row),
            Type::SMALLINT  => $this->getValInteger16($column, $row),
            Type::USMALLINT => $this->getValUinteger16($column, $row),
            Type::BIGINT    => $this->getValInteger64($column, $row),
            Type::FLOAT     => $this->getValFloat($column, $row),
            Type::DOUBLE    => $this->getValDouble($column, $row),
            Type::INVALID   => throw new DuckDBException('Invalid type'),
            default         => throw new DuckDBException('No type found ' . $type->name),
        };
    }

    /**
     * @param int $column
     * @param int $row
     *
     * @return ?int
     */
    private function getValInteger32(int $column, int $row): ?int
    {
        $isNull = $this->duckDBFFI->duckdb_value_is_null(
            $this->duckDBFFI->addr($this->CDataQueryResult),
            $column,
            $row
        );

        if ($isNull) {
            return null;
        }

        return $this->duckDBFFI->duckdb_value_int32(
            $this->duckDBFFI->addr($this->CDataQueryResult),
            $column,
            $row
        );
    }

    /**
     * @param int $column
     * @param int $row
     *
     * @return ?int
     */
    private function getValInteger64(int $column, int $row): ?int
    {
        $isNull = $this->duckDBFFI->duckdb_value_is_null(
            $this->duckDBFFI->addr($this->CDataQueryResult),
            $column,
            $row
        );

        if ($isNull) {
            return null;
        }

        return $this->duckDBFFI->duckdb_value_int64(
            $this->duckDBFFI->addr($this->CDataQueryResult),
            $column,
            $row
        );
    }

    /**
     * @param int $column
     * @param int $row
     *
     * @return bool|null
     */
    private function getValueBoolean(int $column, int $row): ?bool
    {
        $isNull = $this->duckDBFFI->duckdb_value_is_null(
            $this->duckDBFFI->addr($this->CDataQueryResult),
            $column,
            $row
        );

        if ($isNull) {
            return null;
        }

        return $this->duckDBFFI->duckdb_value_boolean(
            $this->duckDBFFI->addr($this->CDataQueryResult),
            $column,
            $row
        );
    }

    /**
     * @param int $column
     * @param int $row
     *
     * @return mixed
     */
    private function getValString(int $column, int $row): mixed
    {
        $value = $this->duckDBFFI->duckdb_value_varchar(
            $this->duckDBFFI->addr($this->CDataQueryResult),
            $column,
            $row
        );
        $val = null;
        if ($value !== null) {
            $val = FFI::string($value);
        }

        $this->duckDBFFI->duckdb_free($value);

        return $val;
    }

    /**
     * @param int $column
     * @param int $row
     *
     * @return float|null
     */
    private function getValFloat(int $column, int $row): ?float
    {
        $isNull = $this->duckDBFFI->duckdb_value_is_null(
            $this->duckDBFFI->addr($this->CDataQueryResult),
            $column,
            $row
        );

        if ($isNull) {
            return null;
        }

        return $this->duckDBFFI->duckdb_value_float(
            $this->duckDBFFI->addr($this->CDataQueryResult),
            $column,
            $row
        );
    }

    /**
     * @param int $column
     * @param int $row
     *
     * @return mixed
     */
    private function getValUinteger32(int $column, int $row): mixed
    {
        $isNull = $this->duckDBFFI->duckdb_value_is_null(
            $this->duckDBFFI->addr($this->CDataQueryResult),
            $column,
            $row
        );

        if ($isNull) {
            return null;
        }

        return $this->duckDBFFI->duckdb_value_uint32(
            $this->duckDBFFI->addr($this->CDataQueryResult),
            $column,
            $row
        );
    }

    private function getValInteger8(int $column, int $row)
    {
        $isNull = $this->duckDBFFI->duckdb_value_is_null(
            $this->duckDBFFI->addr($this->CDataQueryResult),
            $column,
            $row
        );

        if ($isNull) {
            return null;
        }

        return $this->duckDBFFI->duckdb_value_int8(
            $this->duckDBFFI->addr($this->CDataQueryResult),
            $column,
            $row
        );
    }

    private function getValUinteger8(int $column, int $row)
    {
        $isNull = $this->duckDBFFI->duckdb_value_is_null(
            $this->duckDBFFI->addr($this->CDataQueryResult),
            $column,
            $row
        );

        if ($isNull) {
            return null;
        }

        return $this->duckDBFFI->duckdb_value_uint8(
            $this->duckDBFFI->addr($this->CDataQueryResult),
            $column,
            $row
        );
    }

    private function getValInteger16(int $column, int $row)
    {
        $isNull = $this->duckDBFFI->duckdb_value_is_null(
            $this->duckDBFFI->addr($this->CDataQueryResult),
            $column,
            $row
        );

        if ($isNull) {
            return null;
        }

        return $this->duckDBFFI->duckdb_value_int16(
            $this->duckDBFFI->addr($this->CDataQueryResult),
            $column,
            $row
        );
    }

    private function getValUinteger16(int $column, int $row)
    {
        $isNull = $this->duckDBFFI->duckdb_value_is_null(
            $this->duckDBFFI->addr($this->CDataQueryResult),
            $column,
            $row
        );

        if ($isNull) {
            return null;
        }

        return $this->duckDBFFI->duckdb_value_uint16(
            $this->duckDBFFI->addr($this->CDataQueryResult),
            $column,
            $row
        );
    }

    private function getValDouble(int $column, int $row)
    {
        $isNull = $this->duckDBFFI->duckdb_value_is_null(
            $this->duckDBFFI->addr($this->CDataQueryResult),
            $column,
            $row
        );

        if ($isNull) {
            return null;
        }

        return $this->duckDBFFI->duckdb_value_double(
            $this->duckDBFFI->addr($this->CDataQueryResult),
            $column,
            $row
        );
    }
}
