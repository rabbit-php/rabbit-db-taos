<?php
declare(strict_types=1);

namespace Rabbit\DB\Taos;

use DI\DependencyException;
use DI\NotFoundException;
use FFI;
use Rabbit\Base\Contract\InitInterface;
use Rabbit\Base\Helper\ArrayHelper;
use ReflectionException;

/**
 * Class Connection
 * @package Taos
 */
class Connection extends \Rabbit\DB\Connection implements InitInterface
{
    public string $database;
    protected string $host;
    protected int $port;
    public FFI $ffi;
    protected string $commandClass = Command::class;
    protected ?string $libPath = null;

    /**
     * Connection constructor.
     * @param string $dsn
     * @param string $poolKey
     */
    public function __construct(string $dsn, string $poolKey)
    {
        parent::__construct($dsn);
        $this->poolKey = $poolKey;
        isset($this->parseDsn['query']) ? parse_str($this->parseDsn['query'], $this->parseDsn['query']) : $parsed['query'] = [];
        [$this->host, $this->port, $this->username, $this->password, $query] = ArrayHelper::getValueByArray(
            $this->parseDsn,
            ['host', 'port', 'user', 'pass', 'query'],
            ['127.0.0.1', 6030, null, null, []]
        );
        $this->database = (string)ArrayHelper::remove($query, 'dbname');

    }

    public function init(): void
    {
        $this->ffi = TaosHelper::getTaos($this->libPath);
    }


    public function getConn()
    {
        $this->open();
        return parent::getConn();
    }

    /**
     * @return \Rabbit\DB\Schema
     * @throws DependencyException
     * @throws NotFoundException
     * @throws ReflectionException
     */
    public function getSchema(): \Rabbit\DB\Schema
    {
        if ($this->schema !== null) {
            return $this->schema;
        }
        return $this->schema = create([
            'class' => Schema::class,
            'db' => $this
        ]);
    }

    /**
     * @return FFI\CData
     */
    public function createPdoInstance(): FFI\CData
    {
        return $this->ffi->taos_connect($this->host, $this->username, $this->password, $this->database, $this->port);
    }

    /**
     * @param string $name
     * @return string
     */
    public function quoteTableName(string $name): string
    {
        return $name;
    }

    /**
     * @param string $name
     * @return string
     */
    public function quoteColumnName(string $name): string
    {
        return $name;
    }

    /**
     * @param string $value
     * @return string
     */
    public function quoteValue(string $value): string
    {
        return $value;
    }

    /**
     * @param string $sql
     * @return string
     */
    public function quoteSql(string $sql): string
    {
        return $sql;
    }

    /**
     * @param null $conn
     */
    public function setInsertId($conn = null): void
    {
    }
}