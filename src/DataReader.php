<?php
declare(strict_types=1);

namespace Rabbit\DB\Taos;

use FFI;
use Rabbit\Base\Exception\NotSupportedException;
use Rabbit\DB\Command;
use ReflectionException;

/**
 * Class DataReader
 * @package Taos
 */
class DataReader extends \Rabbit\DB\DataReader
{
    protected \FFI\CData $result;
    protected int $fieldsNum;
    protected \FFI\CData $fields;

    /**
     * DataReader constructor.
     * @param Command $command
     * @param array $config
     * @throws ReflectionException
     */
    public function __construct(Command $command, \FFI\CData $result, int $fieldsNum, \FFI\CData $fields, $config = [])
    {
        $this->result = $result;
        $this->fields = $fields;
        $this->fieldsNum = $fieldsNum;
        $this->command = $command;
        $this->statement = $command->db->ffi;
        configure($this, $config);
    }

    /**
     * @param int|string $column
     * @param mixed $value
     * @param int|null $dataType
     * @throws NotSupportedException
     */
    public function bindColumn($column, &$value, int $dataType = null): void
    {
        throw new NotSupportedException("Swoole mysql not support " . __METHOD__);
    }

    /**
     * @param int $mode
     * @throws NotSupportedException
     */
    public function setFetchMode(int $mode): void
    {
        throw new NotSupportedException("Swoole mysql not support " . __METHOD__);
    }

    /**
     * @param int $columnIndex
     * @return mixed|void
     * @throws NotSupportedException
     */
    public function readColumn(int $columnIndex)
    {
        throw new NotSupportedException("Swoole mysql not support " . __METHOD__);
    }

    /**
     * @param string $className
     * @param array $fields
     * @return mixed|void
     * @throws NotSupportedException
     */
    public function readObject(string $className, array $fields)
    {
        throw new NotSupportedException("Swoole mysql not support " . __METHOD__);
    }

    /**
     * @return array|null
     */
    public function read(): ?array
    {
        if (null === $row = $this->statement->taos_fetch_row($this->result)) {
            return null;
        }
        $result = [];
        for ($i = 0; $i < $this->fieldsNum; $i++) {
            $result[FFI::string(FFI::cast('char *', $this->fields[$i]->name))] = TaosHelper::getValue($this->fields[$i]->type, $row[$i]);
        }
        return $result;
    }

    /**
     * @return array
     */
    public function readAll(): array
    {
        $rowData = [];
        $j = 0;
        while (null !== $row = $this->statement->taos_fetch_row($this->result)) {
            for ($i = 0; $i < $this->fieldsNum; $i++) {
                $rowData[$j][FFI::string(FFI::cast('char *', $this->fields[$i]->name))] = TaosHelper::getValue($this->fields[$i]->type, $row[$i]);
            }
            $j++;
        }
        return $rowData;
    }

    /**
     * @return bool
     */
    public function nextResult()
    {
        if (null !== $result = $this->statement->taos_fetch_row($this->result)) {
            $this->index = -1;
            return true;
        }
        return false;
    }

    public function close(): void
    {
        $this->statement->taos_stop_query($this->result);
        $this->statement->taos_free_result($this->result);
        $this->closed = true;
    }

    public function getIsClosed(): bool
    {
        return $this->closed;
    }

    /**
     * @return int
     * @throws NotSupportedException
     */
    public function getRowCount(): int
    {
        throw new NotSupportedException("Swoole mysql not support " . __METHOD__);
    }

    /**
     * @return int
     */
    public function getColumnCount(): int
    {
        return $this->fieldsNum;
    }
}
