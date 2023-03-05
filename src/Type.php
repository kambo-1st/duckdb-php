<?php

namespace Kambo\DuckDB;

enum Type: int
{
    case INVALID = 0;
    case BOOLEAN = 1;
    case TINYINT = 2;
    case SMALLINT = 3;
    case INTEGER = 4;
    case BIGINT = 5;
    case UTINYINT = 6;
    case USMALLINT = 7;
    case UINTEGER = 8;
    case UBIGINT = 9;
    case FLOAT = 10;
    case DOUBLE = 11;
    case TIMESTAMP = 12;
    case DATE = 13;
    case TIME = 14;
    case INTERVAL = 15;
    case HUGEINT = 16;
    case VARCHAR = 17;
    case BLOB = 18;
    case DECIMAL = 19;
    case TIMESTAMP_S = 20;
    case TIMESTAMP_MS = 21;
    case TIMESTAMP_NS = 22;
    case ENUM = 23;
    case LIST = 24;
    case STRUCT = 25;
    case MAP = 26;
    case UUID = 27;
    case UNION = 28;
    case BIT = 29;
}
