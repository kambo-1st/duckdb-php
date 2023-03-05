<?php

namespace Kambo\DuckDB;

use FFI;
use FFI\CData;
use Kambo\DuckDB\Native\DuckDBFFI;
use Kambo\DuckDB\Exception\AppenderException;

/**
 * Appenders are the most efficient way of loading data into DuckDB from within the C interface, and are
 * recommended for fast data loading. The appender is much faster than using prepared statements or individual
 * `INSERT INTO` statements. Appends are made in row-wise format. For every column, a `append[type]`
 * call should be made, after which the row should be finished by calling `duckdb_appender_end_row`.
 * After all rows have been appended, `duckdb_appender_destroy` should be used to finalize the appender
 * and clean up the resulting memory. Note that `duckdb_appender_destroy` should always be called on the
 * resulting appender, even if the function returns `DuckDBError`.
*/
final class Appender
{
    private ?CData $appenderCData;
    private DuckDBFFI $duckDBFFI;

    private bool $open = true;

    /**
     * Creates an appender object.
     *
     * @param Connection $connection The connection context to create the appender in.
     * @param string $table The table name to append to.
     * @param ?string $schema The schema of the table to append to, or `nullptr` for the default schema.
     * @param ?DuckDBFFI $duckDBFFI
     *
     * @return void
     */
    public function __construct(
        Connection $connection,
        string $table,
        string $schema = null,
        ?DuckDBFFI $duckDBFFI = null
    ) {
        if ($duckDBFFI === null) {
            $duckDBFFI = DuckDBFFI::getInstance();
        }

        $this->duckDBFFI = $duckDBFFI;

        $privatePropertyAccessor = function ($prop) {
            return $this->$prop;
        };
        $connectionCData = $privatePropertyAccessor->call($connection, 'connectionCData');

        $appenderCData   = $this->duckDBFFI->new('duckdb_appender');
        $result          = $this->duckDBFFI->duckdb_appender_create(
            $connectionCData,
            $schema,
            $table,
            $this->duckDBFFI->addr($appenderCData)
        );

        $this->appenderCData = $appenderCData;

        if ($result === $this->duckDBFFI->duckDBError()) {
            throw new AppenderException("Failed to create appender:\n" . $this->getErrorMessage());
        }
    }

    /**
     * Appends a boolean value to the given appender.
     *
     * @param bool $value The boolean value to append.
     *
     * @throws AppenderException If the operation fails.
     */
    public function appendBool(bool $value): void
    {
        $this->checkIfOpen();
        $result = $this->duckDBFFI->duckdb_append_bool($this->appenderCData, $value);
        if ($result === $this->duckDBFFI->duckDBError()) {
            throw new AppenderException('Failed to append bool');
        }
    }

    /**
     * Appends an 8-bit integer value to the given appender.
     *
     * @param int $value The 8-bit integer value to append.
     *
     * @throws AppenderException If the operation fails.
     */
    public function appendInt8(int $value): void
    {
        $this->checkIfOpen();
        $result = $this->duckDBFFI->duckdb_append_int8($this->appenderCData, $value);
        if ($result === $this->duckDBFFI->duckDBError()) {
            throw new AppenderException('Failed to append int8');
        }
    }

    /**
     * Appends a 16-bit integer value to the given appender.
     *
     * @param int $value The 16-bit integer value to append.
     *
     * @throws AppenderException If the operation fails.
     */
    public function appendInt16(int $value): void
    {
        $this->checkIfOpen();
        $result = $this->duckDBFFI->duckdb_append_int16($this->appenderCData, $value);
        if ($result === $this->duckDBFFI->duckDBError()) {
            throw new AppenderException('Failed to append int16');
        }
    }

    /**
     * Appends a 32-bit integer value to the given appender.
     *
     * @param int $value
     *
     * @return void
     * @throws AppenderException
     */
    public function appendInt32(int $value): void
    {
        $this->checkIfOpen();
        $result = $this->duckDBFFI->duckdb_append_int32($this->appenderCData, $value);
        if ($result === $this->duckDBFFI->duckDBError()) {
            throw new AppenderException('Failed to append int32');
        }
    }

    /**
     * Appends a 64-bit integer value to the given appender.
     *
     * @param int $value The 64-bit integer value to append.
     *
     * @throws AppenderException If the operation fails.
     */
    public function appendInt64(int $value): void
    {
        $this->checkIfOpen();
        $result = $this->duckDBFFI->duckdb_append_int64($this->appenderCData, $value);
        if ($result === $this->duckDBFFI->duckDBError()) {
            throw new AppenderException('Failed to append int64');
        }
    }

    /**
     * Appends an unsigned 8-bit integer value to the given appender.
     *
     * @param int $value The value to be appended.
     *
     * @throws AppenderException if the append operation fails.
     */
    public function appendUint8(int $value): void
    {
        // Append the uint8_t value to the appender.
        $this->checkIfOpen();
        $result = $this->duckDBFFI->duckdb_append_uint8($this->appenderCData, $value);

        // Throw an exception if the append operation fails.
        if ($result === $this->duckDBFFI->duckDBError()) {
            throw new AppenderException('Failed to append uint8');
        }
    }

    /**
     * Appends an unsigned 16-bit integer value to the given appender.
     *
     * @param int $value The value to be appended.
     *
     * @throws AppenderException if the append operation fails.
     */
    public function appendUint16(int $value): void
    {
        // Append the uint16_t value to the appender.
        $this->checkIfOpen();
        $result = $this->duckDBFFI->duckdb_append_uint16($this->appenderCData, $value);

        // Throw an exception if the append operation fails.
        if ($result === $this->duckDBFFI->duckDBError()) {
            throw new AppenderException('Failed to append uint16');
        }
    }

    /**
     * Appends an unsigned 32-bit integer value to the given appender.
     *
     * @param int $value The value to be appended.
     *
     * @throws AppenderException if the append operation fails.
     */
    public function appendUint32(int $value): void
    {
        $this->checkIfOpen();
        $result = $this->duckDBFFI->duckdb_append_uint32($this->appenderCData, $value);

        // Throw an exception if the append operation fails.
        if ($result === $this->duckDBFFI->duckDBError()) {
            throw new AppenderException('Failed to append uint32');
        }
    }

    /**
     * Appends a float value to a DuckDB appender.
     *
     * @param float $value The float value to append.
     *
     * @throws AppenderException if the append operation fails.
     */
    public function appendFloat(float $value): void
    {
        $this->checkIfOpen();
        $result = $this->duckDBFFI->duckdb_append_float($this->appenderCData, $value);

        // Throw an exception if the append operation fails.
        if ($result === $this->duckDBFFI->duckDBError()) {
            throw new AppenderException('Failed to append float');
        }
    }

    /**
     * Appends a double value to a DuckDB appender.
     *
     * @param float $value The double value to append.
     *
     * @throws AppenderException if the append operation fails.
     */
    public function appendDouble(float $value): void
    {
        $this->checkIfOpen();
        $result = $this->duckDBFFI->duckdb_append_double($this->appenderCData, $value);

        // Throw an exception if the append operation fails.
        if ($result === $this->duckDBFFI->duckDBError()) {
            throw new AppenderException('Failed to append double');
        }
    }

    /**
     * Appends a varchar to a DuckDB appender.
     *
     * @param string $value The varchar value to append.
     *
     * @throws AppenderException if the append operation fails.
     */
    public function appendVarchar(string $value): void
    {
        $this->checkIfOpen();
        $result = $this->duckDBFFI->duckdb_append_varchar($this->appenderCData, $value);

        // Throw an exception if the append operation fails.
        if ($result === $this->duckDBFFI->duckDBError()) {
            throw new AppenderException('Failed to append varchar');
        }
    }

    /**
     * Appends a null to a DuckDB appender.
     *
     * @throws AppenderException if the append operation fails.
     */
    public function appendNull(): void
    {
        $this->checkIfOpen();
        $result = $this->duckDBFFI->duckdb_append_null($this->appenderCData);

        // Throw an exception if the append operation fails.
        if ($result === $this->duckDBFFI->duckDBError()) {
            throw new AppenderException('Failed to append varchar');
        }
    }

    /**
     * Finish the current row of appends. After end_row is called, the next row can be appended.
     * This function should be called after all columns have been appended to the row.
     * After all rows have been appended, Appender::close should be used to finalize the appender.
     *
     * @return void
     * @throws AppenderException
     */
    public function appenderEndRow(): void
    {
        $this->checkIfOpen();
        $result = $this->duckDBFFI->duckdb_appender_end_row($this->appenderCData);
        if ($result === $this->duckDBFFI->duckDBError()) {
            throw new AppenderException(
                'Failed to end row with message: ' . $this->getErrorMessage()
            );
        }

        $this->open = false;
    }

    /**
     * Returns the error message of the last error that occurred.
     *
     * @return string|null
     */
    public function getErrorMessage(): ?string
    {
        return $this->duckDBFFI->duckdb_appender_error($this->appenderCData);
    }

    /**
     * Finalize the appender. After this function is called, the appender is no longer valid.
     *
     * @return void
     */
    public function close(): void
    {
        $result = $this->duckDBFFI->duckdb_appender_destroy($this->duckDBFFI->addr($this->appenderCData));
        if ($result === $this->duckDBFFI->duckDBError()) {
            throw new AppenderException('Failed to close appender');
        }
    }

    /**
     * Returns true if the appender is open.
     *
     * @return bool
     */
    public function isOpen(): bool
    {
        return $this->open;
    }

    /**
     * Destruct object
     */
    public function __destruct()
    {
        if ($this->isOpen()) {
            $this->duckDBFFI->duckdb_appender_destroy($this->duckDBFFI->addr($this->appenderCData));
        }
    }

    /**
     * Check if appender is open
     *
     * @return void
     * @throws AppenderException
     */
    private function checkIfOpen(): void
    {
        if (!$this->open) {
            throw new AppenderException('Appender is not open, create new appender.');
        }
    }
}
