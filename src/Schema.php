<?php
declare(strict_types=1);

namespace Rabbit\DB\Taos;

use DI\DependencyException;
use DI\NotFoundException;
use Psr\SimpleCache\InvalidArgumentException;
use Rabbit\Base\Exception\NotSupportedException;
use Rabbit\DB\ColumnSchema;
use Rabbit\DB\TableSchema;
use ReflectionException;
use Throwable;

/**
 * Class Schema
 * @package Taos
 */
class Schema extends \Rabbit\DB\Schema
{
    const TSDB_DATA_TYPE_NULL = 0;     // 1 bytes
    const TSDB_DATA_TYPE_BOOL = 1;     // 1 bytes
    const TSDB_DATA_TYPE_TINYINT = 2;     // 1 byte
    const TSDB_DATA_TYPE_SMALLINT = 3;     // 2 bytes
    const TSDB_DATA_TYPE_INT = 4;     // 4 bytes
    const TSDB_DATA_TYPE_BIGINT = 5;     // 8 bytes
    const TSDB_DATA_TYPE_FLOAT = 6;     // 4 bytes
    const TSDB_DATA_TYPE_DOUBLE = 7;     // 8 bytes
    const TSDB_DATA_TYPE_BINARY = 8;     // string
    const TSDB_DATA_TYPE_TIMESTAMP = 9;     // 8 bytes
    const TSDB_DATA_TYPE_NCHAR = 10;    // unicode string
    public static array $typeMap = [
        'TIMESTAMP' => self::TYPE_BIGINT,
        'INT' => self::TYPE_INTEGER,
        'BIGINT' => self::TYPE_BIGINT,
        'FLOAT' => self::TYPE_FLOAT,
        'DOUBLE' => self::TYPE_DOUBLE,
        'BINARY' => self::TYPE_CHAR,
        'SMALLINT' => self::TYPE_SMALLINT,
        'TINYINT' => self::TYPE_TINYINT,
        'BOOL' => self::TYPE_BOOLEAN,
        'NCHAR' => self::TYPE_CHAR
    ];

    /**
     * @param string $name
     * @return TableSchema|null
     * @throws DependencyException
     * @throws NotFoundException
     * @throws InvalidArgumentException
     * @throws NotSupportedException
     * @throws ReflectionException
     * @throws Throwable
     */
    protected function loadTableSchema(string $name): ?TableSchema
    {
        $sql = "DESCRIBE $name";
        $result = $this->db->createCommand($sql)->queryAll();
        if ($result) {
            $table = new TableSchema();
            $table->schemaName = $this->db->database;
            $table->name = $name;
            $table->fullName = $table->schemaName . '.' . $table->name;

            foreach ($result as $info) {
                $column = $this->loadColumnSchema($info);
                $table->columns[$column->name] = $column;
            }
            return $table;
        }

        return null;
    }

    /**
     * @param array $info
     * @return ColumnSchema
     * @throws DependencyException
     * @throws NotFoundException|ReflectionException
     */
    protected function loadColumnSchema(array $info): ColumnSchema
    {
        $column = $this->createColumnSchema();
        $column->name = $info['Field'];
        $column->dbType = $info['Type'];
        $column->type = isset(self::$typeMap[$column->dbType]) ? self::$typeMap[$column->dbType] : self::TYPE_STRING;
        $column->phpType = $this->getColumnPhpType($column);
        return $column;
    }

}