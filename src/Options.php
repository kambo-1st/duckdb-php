<?php

namespace Kambo\DuckDB;

final class Options
{
    /**
     * @var array<string, mixed> $options An array of key-value pairs representing the options.
     */
    private array $options = [];

    /**
     * Sets an option to a given value.
     * For possible configuration options looks to: https://duckdb.org/docs/sql/configuration.html
     *
     * @param string $name The name of the option.
     * @param string $value The value of the option.
     * @return self Returns the Options object, to allow method chaining.
     */
    public function set(string $name, string $value): self
    {
        $this->options[$name] = $value;
        return $this;
    }

    /**
     * Gets the value of an option.
     *
     * @param string $name The name of the option.
     * @return mixed Returns the value of the option, or null if not set.
     */
    public function get(string $name): mixed
    {
        return $this->options[$name];
    }

    /**
     * Returns the options as an array.
     *
     * @return array<string, mixed> An array of key-value pairs representing the options.
     */
    public function toArray(): array
    {
        return $this->options;
    }
}
