<?php
declare(strict_types=1);

namespace Rabbit\DB\Taos;

use FFI;
use Psr\SimpleCache\CacheInterface;
use Psr\SimpleCache\InvalidArgumentException;
use Rabbit\Base\App;
use Rabbit\DB\DataReader;
use Rabbit\DB\Exception;
use Throwable;

/**
 * Class Command
 * @package Taos
 */
class Command extends \Rabbit\DB\Command
{
    const FETCH = 'fetch';
    const FETCH_ALL = 'fetchAll';
    const FETCH_COLUMN = 'fetchColumn';
    const FETCH_SCALAR = 'fetchScalar';

    public int $fetchMode = 0;
    private ?int $executed = null;

    /**
     * @param array $values
     * @return $this
     */
    public function bindValues(array $values): self
    {
        if (empty($values)) {
            return $this;
        }
        foreach ($values as $name => $value) {
            if (is_array($value)) {
                $this->params[$name] = json_encode($value, JSON_UNESCAPED_UNICODE);
            } else {
                $this->params[$name] = $value;
            }
        }

        return $this;
    }

    /**
     * @param bool|null $forRead
     * @throws Exception
     */
    public function prepare(bool $forRead = null)
    {
        if ($this->pdoStatement) {
            $this->bindPendingParams();
            return;
        }
        $pdo = $this->db->getConn();
        if ($pdo === null) {
            throw new Exception('Can not get the connection!');
        }
        try {
            $this->pdoStatement = $this->db->ffi->taos_stmt_init($pdo);
            $this->db->ffi->taos_stmt_prepare($this->pdoStatement, $this->sql, 0);
            $this->bindPendingParams();
        } catch (Throwable $e) {
            $message = $e->getMessage() . " Failed to prepare SQL: $this->sql";
            $e = new Exception($message, null, (int)$e->getCode(), $e);
            throw $e;
        }
    }

    protected function bindPendingParams(): void
    {
        $argType = FFI::arrayType($this->db->ffi->type('TAOS_BIND'), [count($this->params)]);
        $p = $this->db->ffi->new($argType);
        $i = 0;
        foreach ($this->params as $name => $value) {
            if (is_int($value)) {
                $v = FFI::new('int');
                $v->cdata = $value;
                $p[$i]->buffer_type = Schema::TSDB_DATA_TYPE_INT;
                $p[$i]->buffer_length = FFI::sizeof($v);
            } elseif (is_string($value)) {
                $v = FFI::string(FFI::new('char *'), strlen($value));
                $v->cdata = $value;
                $p[$i]->buffer_type = Schema::TSDB_DATA_TYPE_NCHAR;
                $p[$i]->buffer_length = strlen($v->cdata);
            } elseif (is_bool($value)) {
                $v = FFI::new('bool');
                $v->cdata = $value;
                $p[$i]->buffer_type = Schema::TSDB_DATA_TYPE_BOOL;
                $p[$i]->buffer_length = FFI::sizeof($v);
            } elseif (is_float($value)) {
                $v = FFI::new('float');
                $v->cdata = $value;
                $p[$i]->buffer_type = Schema::TSDB_DATA_TYPE_FLOAT;
                $p[$i]->buffer_length = FFI::sizeof($v);
            } elseif (is_double($value)) {
                $v = FFI::new('double');
                $v->cdata = $value;
                $p[$i]->buffer_type = Schema::TSDB_DATA_TYPE_DOUBLE;
                $p[$i]->buffer_length = FFI::sizeof($v);
            }
            $p[$i]->buffer = FFI::addr($v);
            $p[$i]->length = FFI::addr($p[$i]->buffer_length);
            $i++;
        }
    }

    /**
     * @return int
     * @throws Throwable
     */
    public function execute(): int
    {
        $rawSql = $this->getRawSql();
        $this->logQuery($rawSql, 'clickhouse');
        try {
            $result = $this->db->ffi->taos_query($this->db->getConn(), $rawSql);
            $code = $this->db->ffi->taos_errno($result);
            if ($code !== 0) {
                throw new Exception(sprintf("execute $rawSql failed, reason:%s", FFI::string($this->db->ffi->taos_errstr($result))));
            }
            return FFI::cast('int', $this->db->ffi->taos_affected_rows($result))->cdata;
        } catch (Throwable $exception) {
            throw new Exception($exception->getMessage());
        } finally {
            $this->db->ffi->taos_free_result($result);
        }
    }


    /**
     * @return array|null
     * @throws InvalidArgumentException
     * @throws Throwable
     */
    public function queryColumn(): ?array
    {
        return $this->queryInternal(self::FETCH_COLUMN);
    }

    /**
     * @return string|null
     * @throws InvalidArgumentException
     * @throws Throwable
     */
    public function queryScalar(): ?string
    {
        $result = $this->queryInternal(self::FETCH_SCALAR, 0);
        if (is_array($result)) {
            return current($result);
        } else {
            return $result === null ? null : (string)$result;
        }
    }

    /**
     * @param string $method
     * @param int|null $fetchMode
     * @return array|mixed|DataReader
     * @throws Exception
     * @throws Throwable|InvalidArgumentException
     */
    protected function queryInternal(string $method, int $fetchMode = null)
    {
        $rawSql = $this->getRawSql();

        if ($method !== '') {
            $info = $this->db->getQueryCacheInfo($this->queryCacheDuration, $this->cache);
            if (is_array($info)) {
                /** @var CacheInterface $cache */
                $cache = $info[0];
                $cacheKey = array_filter([
                    __CLASS__,
                    $method,
                    $fetchMode,
                    $this->db->dsn,
                    $rawSql,
                ]);
                if (!empty($ret = $cache->get($cacheKey))) {
                    $result = unserialize($ret);
                    if (is_array($result) && isset($result[0])) {
                        $this->logQuery($rawSql . '; [Query result served from cache]', 'taos');
                        return $this->prepareResult($result[0], $method);
                    }
                }
            }
        }

        $this->logQuery($rawSql, 'taos');

        try {
            $taos = $this->db->getConn();
            $res = $this->db->ffi->taos_query($taos, $rawSql);
            $fieldsNum = FFI::cast('int', $this->db->ffi->taos_num_fields($res));
            $fields = $this->db->ffi->taos_fetch_fields($res);
            if ($method === '') {
                return new \Taos\DataReader($this, $res, $fieldsNum->cdata, $fields);
            }
            $rowData = [];
            $j = 0;
            while (null !== $row = $this->db->ffi->taos_fetch_row($res)) {
                for ($i = 0; $i < $fieldsNum->cdata; $i++) {
                    $rowData[$j][FFI::string(FFI::cast('char *', $fields[$i]->name))] = TaosHelper::getValue($fields[$i]->type, $row[$i]);
                }
                $j++;
            }
            $result = $this->prepareResult($rowData, $method);
            if (isset($cache, $cacheKey, $info)) {
                !$cache->has($cacheKey) && $cache->set((string)$cacheKey, serialize([$result]), $info[1]) && App::debug(
                    'Saved query result in cache',
                    'taos'
                );
            }
            return $result;
        } catch (Exception $e) {
            throw new Exception("Query error: " . $e->getMessage());
        } finally {
            $this->db->ffi->taos_free_result($res);
        }
    }

    /**
     * @param array $result
     * @param string|null $method
     * @return array|mixed
     */
    private function prepareResult(array $result, string $method = null): array
    {
        switch ($method) {
            case self::FETCH_COLUMN:
                return array_map(function ($a) {
                    return array_values($a)[0];
                }, $result);
            case self::FETCH_SCALAR:
                if (array_key_exists(0, $result)) {
                    return current($result[0]);
                }
                break;
            case self::FETCH:
                return array_shift($result);
        }

        return $result;
    }
}