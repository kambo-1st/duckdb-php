<?php

require __DIR__ . '/../../../vendor/autoload.php';

$database = new Kambo\DuckDB\Database();
$connection = new Kambo\DuckDB\Connection($database);

$connection->query('CREATE TABLE people(id INTEGER, name VARCHAR);');


$appender = new Kambo\DuckDB\Appender($connection, 'people');

$appender->appendInt32(2147483647);
$appender->appendVarchar('Here i am');

$appender->appenderEndRow();
$appender->close();

$result = $connection->query('SELECT * FROM people;');
var_export($result->toArray());
