<?php

require __DIR__ . '/../../../vendor/autoload.php';

$database = new Kambo\DuckDB\Database();
$connection = new Kambo\DuckDB\Connection($database);

$connection->query('CREATE TABLE integers(i INTEGER, j INTEGER);');
$connection->query('INSERT INTO integers VALUES (3,4), (5,6), (7, NULL) ');

$result = $connection->query('SELECT * FROM integers;');

var_export($result->toArray());
