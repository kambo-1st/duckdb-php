# DuckDB PHP library


## changes

* Merge Pull Request : https://github.com/kambo-1st/duckdb-php/pull/6

An unofficial  [DuckDB]( https://duckdb.org/) communication library for PHP.

[![Latest Version on Packagist](https://img.shields.io/packagist/v/kambo/duckdb-php.svg?style=flat-square)](https://packagist.org/packages/kambo/duckdb-php)
[![Tests](https://img.shields.io/github/actions/workflow/status/kambo-1st/duckdb-php/run-tests.yml?branch=main&label=tests&style=flat-square)](https://github.com/kambo-1st/duckdb-php/actions/workflows/run-tests.yml)
[![Total Downloads](https://img.shields.io/packagist/dt/kambo/duckdb-php.svg?style=flat-square)](https://packagist.org/packages/kambo/duckdb-php)


## Installation

You can install the package via composer:

```bash
composer require kambo/duckdb kambo/duckdb-php-linux-lib
```

Note: the kambo/duckdb-php-linux-lib package contains a binary library for Linux.

## Usage

```php
$database = new Kambo\DuckDB\Database();
$connection = new Kambo\DuckDB\Connection($database);

$connection->query('CREATE TABLE integers(i INTEGER, j INTEGER);');
$connection->query('INSERT INTO integers VALUES (3,4), (5,6), (7, NULL) ');

$result = $connection->query('SELECT * FROM integers;');

var_export($result->toArray());
```

## Current limitations

- This library is in alpha version.
- It currently only works under Linux (work is ongoing for other platforms).
- It has limited support of DuckDB APIs.

## Testing

```bash
composer test
```

## Changelog

Please see [CHANGELOG](CHANGELOG.md) for more information on what has changed recently.

## License

The MIT License (MIT). Please see [License File](LICENSE.md) for more information.
