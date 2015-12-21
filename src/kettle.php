<?php

/**
 * Kettle
 *
 * http://github.com/inouet/kettle/
 *
 * Kettle is a lightweight object-dynamodb mapper for PHP.
 *
 * License
 *   This software is released under the MIT License, see LICENSE.txt.
 *
 * @package Kettle
 * @author  Taiji Inoue <inudog@gmail.com>
 */

namespace Kettle;

use Aws\Credentials\Credentials;
use Aws\DynamoDb\DynamoDbClient;

class ORM
{

    const DEFAULT_CONNECTION = 'default';

    // --------------------------

    /**
     * Class configuration
     *
     * @var array
     *          - key
     *          - secret
     *          - region
     *          - endpoint/base_url  (for DynamoDB local)
     *          - version
     *          - scheme
     *          - profile   (AWS Credential Name)
     */
    protected static $_config_default = array(
        'key'      => null,
        'secret'   => null,
        'region'   => null,
        'base_url' => null,
        'endpoint' => null,
        'version'  => '2012-08-10',
        'scheme'   => 'https',
        'profile'  => null,
    );

    protected static $_config = array();

    /**
     * @var \Aws\DynamoDb\DynamoDbClient[]
     */
    protected static $_client = array();

    // Log of all queries run, mapped by connection key, only populated if logging is enabled
    protected static $_query_log = array();

    // --------------------------

    // DynamoDB TableName
    protected $_table_name;

    // HashKey
    protected $_hash_key;

    // RangeKey
    protected $_range_key;

    // ConnectionName
    protected $_connection_name = self::DEFAULT_CONNECTION;

    /**
     * data schema
     *
     * @var array
     *
     *        ex)
     *        $_schema = array(
     *               'field_name_1' => 'S',
     *               'field_name_2' => 'N',
     *               );
     */
    protected $_schema = array();

    /**
     * DynamoDB record data is stored here as an associative array
     *
     * @var array
     */
    protected $_data = array();

    protected $_data_original = array();

    // LIMIT (QueryParameter)
    protected $_limit = null;

    // ExclusiveStartKey (QueryParameter)
    protected $_exclusive_start_key = null;

    // IndexName (QueryParameter)
    protected $_query_index_name = null;

    // ConsistentRead (QueryParameter)
    protected $_consistent_read = false;

    // LastEvaluatedKey (QueryResponse)
    protected $_last_evaluated_key = null;

    // Count (QueryResponse)
    public $_result_count = null;

    /**
     * Array of WHERE clauses (QueryParameter)
     *
     * $_where_conditions = array(
     *    0 => array('name', 'EQ', 'John'),
     *    1 => array('age',  'GT', 20),
     *  );
     */
    protected $_where_conditions = array();

    /**
     * Array of Filter clauses (QueryParameter)
     *
     * $_filter_conditions = array(
     *    0 => array('country', 'IN', array('Japan', 'Korea'))
     *    1 => array('age',  'GT', 20),
     *  );
     */
    protected $_filter_conditions = array();

    // Is this a new object (has create() been called)?
    protected $_is_new = false;

    //-----------------------------------------------
    // PUBLIC METHODS
    //-----------------------------------------------
    public static function configure($key, $value, $connection_name = self::DEFAULT_CONNECTION)
    {
        if (!isset(self::$_config[$connection_name])) {
            self::$_config[$connection_name] = self::$_config_default;
        }
        self::$_config[$connection_name][$key] = $value;
    }

    /**
     * Retrieve configuration options by key, or as whole array.
     *
     * @param string $key
     *
     * @return string|array
     */
    public static function getConfig($key = null, $connection_name = self::DEFAULT_CONNECTION)
    {
        if ($key) {
            return isset(self::$_config[$connection_name][$key]) ? self::$_config[$connection_name][$key] : null;
        } else {
            return self::$_config[$connection_name];
        }
    }

    /**
     * Get an array containing all the queries and response
     * Only works if the 'logging' config option is
     * set to true. Otherwise, returned array will be empty.
     *
     * @return array
     * @deprecated
     */
    public static function getQueryLog()
    {
        if (isset(self::$_query_log)) {
            return self::$_query_log;
        }
        return array();
    }

    /**
     * Get last query
     *
     * @return array
     * @deprecated
     */
    public static function getLastQuery()
    {
        if (!isset(self::$_query_log)) {
            return array();
        }
        return end(self::$_query_log);
    }

    /**
     * Get connection name
     *
     * @return string
     */
    public function getConnectionName()
    {
        return $this->_connection_name;
    }

    public function setConnectionName($connection_name)
    {
        $this->_connection_name = $connection_name;
    }

    /**
     * Get number of records that matched the query
     *
     * @return int
     */
    public function getCount()
    {
        return $this->_result_count;
    }

    /**
     * Retrieve single result using hash_key and range_key
     *
     * @param  string $hash_key_value
     * @param  string $range_key_value
     * @param  array  $options
     *
     * @return $this instance of the ORM sub class
     * @throws \Exception
     */
    public function findOne($hash_key_value, $range_key_value = null, array $options = array())
    {
        $conditions = array(
            $this->_hash_key => $hash_key_value,
        );

        if ($range_key_value) {
            if (!$this->_range_key) {
                throw new \Exception("Range key is not defined.");
            }
            $conditions[$this->_range_key] = $range_key_value;
        }

        $key  = $this->_formatAttributes($conditions);
        $args = array(
            'TableName'              => $this->_table_name,
            'Key'                    => $key,
            'ConsistentRead'         => $this->_consistent_read,
            'ReturnConsumedCapacity' => 'TOTAL',
            // 'AttributesToGet'
        );


        // Merge $options to $args
        $option_names = array('AttributesToGet', 'ReturnConsumedCapacity');
        foreach ($option_names as $option_name) {
            if (isset($options[$option_name])) {
                $args[$option_name] = $options[$option_name];
            }
        }

        $_client = $this->getClient();
        $item    = $_client->getItem($args);

        if (!is_array($item['Item'])) {
            return null;
        }

        $result = $this->_formatResult($item['Item']);

        $class_name = get_called_class();
        $instance   = self::factory($class_name, $this->_connection_name);
        $instance->hydrate($result);
        return $instance;
    }

    /**
     * Retrieve multiple results using query
     *
     * @param  array $options
     *
     * @return $this[]
     */
    public function findMany(array $options = array())
    {
        $conditions = $this->_buildConditions($this->_where_conditions);
        if ($this->_filter_conditions) {
            $filter_conditions      = $this->_buildConditions($this->_filter_conditions);
            $options['QueryFilter'] = $filter_conditions;
        }
        $result = $this->query($conditions, $options);

        // scan($tableName, $filter, $limit = null)
        $array      = array();
        $class_name = get_called_class();
        foreach ($result as $row) {
            $instance = self::factory($class_name, $this->_connection_name);
            $instance->hydrate($row);
            $array[] = $instance;
        }
        return $array;
    }

    /**
     * Retrieve first result using query
     *
     * @param  array $options
     *
     * @return $this|null
     */
    public function findFirst(array $options = array())
    {
        // $this->_limit = 1; # FIX: bug at using filter
        $array = $this->findMany($options);
        if (is_array($array) && sizeof($array) > 0) {
            return $array[0];
        }
        return null;
    }

    /**
     * Save data to the DynamoDB
     *
     * @param array $options
     *
     *  $options = array(
     *     'ReturnValues'                => 'NONE', // NONE|ALL_OLD|UPDATED_OLD|ALL_NEW|UPDATED_NEW
     *     'ReturnConsumedCapacity'      => 'NONE', // INDEXES|TOTAL|NONE
     *     'ReturnItemCollectionMetrics' => 'NONE', // SIZE|NONE
     *
     *     'ForceUpdate'                 => false, // If true No ConditionalCheck
     *  );
     *
     * @return \Aws\Result
     */
    public function save(array $options = array())
    {
        $values   = $this->_compact($this->_data);
        $expected = array();

        if ($this->_is_new) { // insert
            if (!isset($options['ForceUpdate']) || !$options['ForceUpdate']) {
                // Expect duplicate error if already exists.
                $exists = array();
                foreach ($this->_schema as $key => $value) {
                    $exists[$key] = false;
                }
                $options['Exists'] = $exists;
            }
            $result        = $this->putItem($values, $options, $expected);
            $this->_is_new = false;
        } else { // update
            if (!isset($options['ForceUpdate']) || !$options['ForceUpdate']) {
                // If data is modified by different instance or process,
                // throw Aws\DynamoDb\Exception\ConditionalCheckFailedException
                $expected = $this->_data_original;
            }
            $result = $this->putItem($values, $options, $expected);
            //$result = $this->updateItem($values, $options, $expected);
        }

        return $result;
    }

    /**
     * Delete record
     *
     * @return mixed
     * @link http://docs.aws.amazon.com/aws-sdk-php/latest/class-Aws.DynamoDb.DynamoDbClient.html#_deleteItem
     */
    public function delete()
    {
        $conditions = $this->_getKeyConditions();
        $args       = array(
            'TableName'    => $this->_table_name,
            'Key'          => $conditions,
            'ReturnValues' => 'ALL_OLD',
        );

        $_client = $this->getClient();
        $result  = $_client->deleteItem($args);
        return $result;
    }

    /**
     * Add a LIMIT to the query
     *
     * @param  int $limit
     *
     * @return $this
     */
    public function limit($limit)
    {
        $this->_limit = $limit;
        return $this;
    }

    /**
     * Set IndexName (Query Parameter)
     *
     * @param  string $index_name
     *
     * @return $this
     */
    public function index($index_name)
    {
        $this->_query_index_name = $index_name;
        return $this;
    }

    /**
     * Set ConsistentRead Option to the query
     *
     * @param bool $consistent_read
     *
     * @return $this
     */
    public function consistent($consistent_read = true)
    {
        $this->_consistent_read = $consistent_read;
        return $this;
    }

    /**
     * The LastEvaluatedKey is only provided if the results exceed 1 MB, or if you have used Limit.
     *
     * @return mixed array|null
     */
    public function getLastEvaluatedKey()
    {
        return $this->_last_evaluated_key;
    }

    /**
     * Add a WHERE column = value clause
     *
     * @param string $key
     * @param string $value or $operator
     * @param mixed  $value
     *
     * Usage:
     *    $user->where('name', 'John');
     *    $user->where('age', '>', 20);
     *
     * @return $this
     */
    public function where()
    {
        $args = func_get_args();
        $key  = $args[0];
        if (func_num_args() == 2) {
            $value    = $args[1];
            $operator = 'EQ';
        } else {
            $value    = $args[2];
            $operator = $this->_convertOperator($args[1]);
        }

        $this->_where_conditions[] = array($key, $operator, $value);
        return $this;
    }

    /**
     * Add a Filter column = value clause
     *
     * @param string $key
     * @param string $value or $operator
     * @param mixed  $value
     *
     * Usage:
     *    $user->filter('name', 'John');
     *    $user->filter('age', '>', 20);
     *
     * @return $this
     */
    public function filter()
    {
        $args = func_get_args();
        $key  = $args[0];
        if (func_num_args() == 2) {
            $value    = $args[1];
            $operator = 'EQ';
        } else {
            $value    = $args[2];
            $operator = $this->_convertOperator($args[1]);
        }

        $this->_filter_conditions[] = array($key, $operator, $value);
        return $this;
    }

    /**
     * Set a property to a particular value on this object.
     *
     * @param string $key
     * @param mixed  $value
     */
    public function set($key, $value)
    {
        if (array_key_exists($key, $this->_schema)) {
            $type = $this->_getDataType($key);
            if ($type == 'S' || $type == 'N') {
                $value = strval($value);
            }
            $this->_data[$key] = $value;
        }
    }

    /**
     * Return the value of a property of this object (dynamodb row) or null if not present.
     *
     * @param string $key
     *
     * @return mixed
     */
    public function get($key)
    {
        return isset($this->_data[$key]) ? $this->_data[$key] : null;
    }

    /**
     * Remove value from set type field (String Set, Number Set, Binary Set)
     *
     * @param $key
     * @param $value
     */
    public function setRemove($key, $value)
    {
        $type = $this->_getDataType($key);
        if ($type == 'SS' || $type == 'NS' || $type == 'BS') {
            $array = $this->get($key);
            $index = array_search($value, $array);
            if (!is_null($index)) {
                unset($array[$index]);
            }
            $array = array_values($array);
            $this->set($key, $array);
        }
    }

    /**
     * Add value to set type field (String Set, Number Set, Binary Set)
     *
     * @param $key
     * @param $value
     */
    public function setAdd($key, $value)
    {
        $type = $this->_getDataType($key);
        if ($type == 'SS' || $type == 'NS') {
            $this->_data[$key][] = strval($value);
        } elseif ($type == 'BS') {
            $this->_data[$key][] = $value;
        }
    }

    /**
     * @param array $data
     *
     * @return $this
     */
    public function create(array $data = array())
    {
        $this->_is_new = true;
        return $this->hydrate($data);
    }

    /**
     * @param array $data
     *
     * @return $this
     */
    public function hydrate(array $data = array())
    {
        foreach ($data as $key => $value) {
            $this->set($key, $value);
        }
        $this->_data_original = $this->_data;
        return $this;
    }

    /**
     * Return the raw data wrapped by this ORM instance as an associative array.
     *
     * @return array
     */
    public function asArray()
    {
        return $this->_data;
    }

    /**
     * Return DynamoDbClient instance
     *
     * @return \Aws\DynamoDb\DynamoDbClient
     */
    public function getClient()
    {
        $client = self::$_client[$this->_connection_name];
        return $client;
    }

    /**
     * query
     *
     * @param  array $conditions
     * @param  array $options
     *
     * @return array
     *
     * @link http://docs.aws.amazon.com/aws-sdk-php/latest/class-Aws.DynamoDb.DynamoDbClient.html#_query
     */
    public function query(array $conditions, array $options = array())
    {
        $args = array(
            'TableName'              => $this->_table_name,
            'KeyConditions'          => $conditions,
            'ScanIndexForward'       => true,
            // Select: ALL_ATTRIBUTES|ALL_PROJECTED_ATTRIBUTES|SPECIFIC_ATTRIBUTES|COUNT
            'Select'                 => 'ALL_ATTRIBUTES',
            'ReturnConsumedCapacity' => 'TOTAL',
            'ConsistentRead'         => $this->_consistent_read,
            //'AttributesToGet'
            //'ExclusiveStartKey'
            //'IndexName'
        );

        // Merge $options to $args
        $option_names = array('ScanIndexForward', 'QueryFilter');
        foreach ($option_names as $option_name) {
            if (isset($options[$option_name])) {
                $args[$option_name] = $options[$option_name];
            }
        }

        // if IndexName is specified
        if ($this->_query_index_name) {
            $args['IndexName'] = $this->_query_index_name;
        }

        if (intval($this->_limit) > 0) { // Has limit
            // if ExclusiveStartKey is set
            if ($this->_exclusive_start_key) {
                $exclusive_start_key       = $this->_formatAttributes($this->_exclusive_start_key);
                $args['ExclusiveStartKey'] = $exclusive_start_key;
            }

            $args['Limit'] = intval($this->_limit);

            $_client = $this->getClient();
            $result  = $_client->query($args);

            // $result is "Guzzle\Service\Resource\Model"
            // and $result has next keys
            // - Count
            // - Items
            // - ScannedCount
            // - LastEvaluatedKey
            $items = $result['Items'];

            // Set LastEvaluatedKey
            $last_evaluated_key = null;
            if (isset($result['LastEvaluatedKey'])) {
                $last_evaluated_key = $this->_formatResult($result['LastEvaluatedKey']);
            }
            $this->_last_evaluated_key = $last_evaluated_key;

            // Set Count
            $result_count = null;
            if (isset($result['Count'])) {
                $result_count = $result['Count'];
            }
            $this->_result_count = $result_count;

        } else { // No limit (Use Iterator)
            $_client  = $this->getClient();
            $iterator = $_client->getIterator('Query', $args);

            // $iterator is "Aws\Common\Iterator\AwsResourceIterator"
            $items = array();
            foreach ($iterator as $item) {
                $items[] = $item;
            }

            // Set Count
            $this->_result_count = count($items);

        }

        return $this->_formatResults($items);
    }

    /**
     * Retrieve all records using scan
     *
     * @param  array $options
     *
     * @return $this[]
     */
    public function findAll(array $options = array())
    {
        if ($this->_filter_conditions) {
            $filter_conditions     = $this->_buildConditions($this->_filter_conditions);
            $options['ScanFilter'] = $filter_conditions;
        }
        $result     = $this->scan($options);
        $array      = array();
        $class_name = get_called_class();
        foreach ($result as $row) {
            $instance = self::factory($class_name, $this->_connection_name);
            $instance->hydrate($row);
            $array[] = $instance;
        }
        return $array;
    }

    /**
     * scan
     *
     * @param array $options
     *
     * @return array
     *
     */
    public function scan(array $options = array())
    {
        $options['TableName'] = $this->_table_name;
        $_client              = $this->getClient();

        $iterator = $_client->getIterator('Scan', $options);
        $items    = array();
        foreach ($iterator as $item) {
            $items[] = $item;
        }
        return $this->_formatResults($items);
    }

    /**
     * putItem
     *
     * @param array $values
     * @param array $options
     * @param array $expected
     *
     * @return \Aws\Result
     *
     * @link http://docs.aws.amazon.com/aws-sdk-php/latest/class-Aws.DynamoDb.DynamoDbClient.html#_putItem
     */
    public function putItem(array $values, array $options = array(), array $expected = array())
    {
        $args = array(
            'TableName'                   => $this->_table_name,
            'Item'                        => $this->_formatAttributes($values),
            //'ReturnValues'                => 'ALL_NEW',
            'ReturnConsumedCapacity'      => 'TOTAL',
            'ReturnItemCollectionMetrics' => 'SIZE',
        );

        // Set Expected if exists
        if ($expected || isset($options['Exists'])) {
            $exists           = isset($options['Exists']) ? $options['Exists'] : array();
            $args['Expected'] = $this->_formatAttributeExpected($expected, $exists);
        }

        // Merge $options to $args
        $option_names = array('ReturnValues', 'ReturnConsumedCapacity', 'ReturnItemCollectionMetrics');
        foreach ($option_names as $option_name) {
            if (isset($options[$option_name])) {
                $args[$option_name] = $options[$option_name];
            }
        }

        $_client = $this->getClient();
        $item    = $_client->putItem($args);

        return $item;
    }

    /**
     * updateItem
     *
     * @param array $values associative array
     *
     * $values = array(
     *     'name' => 'John',
     *     'age'  => 30,
     * );
     *
     * @param array $options
     *
     * $options = array(
     *     'ReturnValues'                => 'string',
     *     'ReturnConsumedCapacity'      => 'string',
     *     'ReturnItemCollectionMetrics' => 'string',
     *     'Action'                      => array('age' => 'ADD'),
     *     'Exists'                      => array('age' => true),
     * );
     *
     * @param array $expected
     *
     * @return \AWS\Result
     *
     * @link http://docs.aws.amazon.com/aws-sdk-php/latest/class-Aws.DynamoDb.DynamoDbClient.html#_updateItem
     */
    public function updateItem(array $values, array $options = array(), array $expected = array())
    {
        $conditions = $this->_getKeyConditions();

        $action = array(); // Update Action (ADD|PUT|DELETE)
        if (isset($options['Action'])) {
            $action = $options['Action'];
        }

        $attributes = $this->_formatAttributeUpdates($values, $action);

        foreach ($conditions as $key => $value) {
            if (isset($attributes[$key])) {
                unset($attributes[$key]);
            }
        }
        $args = array(
            'TableName'                   => $this->_table_name,
            'Key'                         => $conditions,
            'AttributeUpdates'            => $attributes,
            'ReturnValues'                => 'ALL_NEW',
            'ReturnConsumedCapacity'      => 'TOTAL',
            'ReturnItemCollectionMetrics' => 'SIZE',
        );

        // Set Expected if exists
        if ($expected || isset($options['Exists'])) {
            $exists           = isset($options['Exists']) ? $options['Exists'] : array();
            $args['Expected'] = $this->_formatAttributeExpected($expected, $exists);
        }

        // Merge $options to $args
        $option_names = array('ReturnValues', 'ReturnConsumedCapacity', 'ReturnItemCollectionMetrics');
        foreach ($option_names as $option_name) {
            if (isset($options[$option_name])) {
                $args[$option_name] = $options[$option_name];
            }
        }

        $_client = $this->getClient();
        $item    = $_client->updateItem($args);

        return $item;
    }

    /**
     * Set ExclusiveStartKey for query parameter
     *
     * @param array $exclusive_start_key
     *
     *              $exclusive_start_key = array(
     *                                       'key_name1' => 'value1',
     *                                       'key_name2' => 'value2',
     *                                     );
     *
     * @return $this
     */
    public function setExclusiveStartKey(array $exclusive_start_key)
    {
        $this->_exclusive_start_key = $exclusive_start_key;
        return $this;
    }

    /**
     * Reset Where Conditions and Limit ..
     *
     * @return $this
     */
    public function resetConditions()
    {
        $this->_limit               = null;
        $this->_where_conditions    = array();
        $this->_filter_conditions   = array();
        $this->_exclusive_start_key = null;
        $this->_query_index_name    = null;
        $this->_consistent_read     = false;
        return $this;
    }

    /**
     * Get Hash Key
     *
     * @return string
     */
    public function getHashKey()
    {
        return $this->_hash_key;
    }

    /**
     * Get Range Key
     *
     * @return string
     */
    public function getRangeKey()
    {
        return $this->_range_key;
    }

    /**
     * Get Table Name
     *
     * @return string
     */
    public function getTableName()
    {
        return $this->_table_name;
    }

    /**
     * Convert to DynamoDB Import/Export Format.
     *
     * @link http://docs.aws.amazon.com/datapipeline/latest/DeveloperGuide/dp-importexport-ddb-pipelinejson-verifydata2.html
     *
     * @return string
     */
    public function toImportFormat()
    {
        $text = "";
        $etx  = chr(3); // ETX
        $stx  = chr(2); // STX

        foreach ($this->_schema as $column => $type) {
            $value = $this->get($column);
            if (strlen($value) == 0) {
                continue;
            }
            // column_name≤ETX>{type:value}<STX>
            $data = array(strtolower($type) => $value);
            $json = json_encode($data);
            if (!$json) {
                continue;
            }
            $text .= $column . $etx . $json . $stx;
        }
        $text = rtrim($text, $stx); // remove last STX
        $text .= "\n";
        return $text;
    }

    /**
     * Retrieve items in batches of up to 100
     *
     * @param array $key_values
     *
     *     HashKey:            [hash_key_value1, hash_key_value2 ..]
     *     HashKey + RangeKey: [[hash_key_value1, range_key_value1] ...]
     *
     * @return $this[]
     */
    public function batchGetItems(array $key_values)
    {
        $keys = array();
        foreach ($key_values as $key_value) {
            if ($this->_range_key) {
                $conditions = array(
                    $this->_hash_key  => $key_value[0],
                    $this->_range_key => $key_value[1]
                );
            } else {
                $_id        = is_array($key_value) ? $key_value[0] : $key_value;
                $conditions = array(
                    $this->_hash_key => $_id
                );
            }
            $keys[] = $this->_formatAttributes($conditions);
        }
        $_client          = $this->getClient();
        $result           = $_client->batchGetItem(
            array(
                'RequestItems' => array(
                    $this->_table_name => array(
                        'Keys'           => $keys,
                        'ConsistentRead' => true
                    )
                )
            )
        );
        $items            = $result->getPath("Responses/{$this->_table_name}");
        $class_name       = get_called_class();
        $formatted_result = $this->_formatResults($items);

        $array = array();
        foreach ($formatted_result as $row) {
            $instance = self::factory($class_name, $this->_connection_name);
            $instance->hydrate($row);
            $array[] = $instance;
        }
        return $array;
    }

    /**
     * Retrieve findMany result as array
     *
     * @param array $options
     *
     * @return array
     */
    public function findArray(array $options = array())
    {
        $entity_list = $this->findMany($options);
        $result      = array();
        foreach ($entity_list as $entity) {
            $result[] = $entity->asArray();
        }
        return $result;
    }

    //-----------------------------------------------
    // MAGIC METHODS
    //-----------------------------------------------

    public function __get($key)
    {
        return $this->get($key);
    }

    public function __set($key, $value)
    {
        $this->set($key, $value);
    }

    public function __unset($key)
    {
        unset($this->_data[$key]);
    }

    public function __isset($key)
    {
        return isset($this->_data[$key]);
    }

    //-----------------------------------------------
    // PUBLIC METHODS (STATIC)
    //-----------------------------------------------

    /**
     * @param string $class_name
     *
     * @return $this instance of the ORM sub class
     */
    public static function factory($class_name, $connection_name = self::DEFAULT_CONNECTION)
    {
        self::_setupClient($connection_name);

        /** @var self $object */
        $object = new $class_name();
        $object->setConnectionName($connection_name);
        return $object;
    }

    //-----------------------------------------------
    // PRIVATE METHODS
    //-----------------------------------------------
    protected function __construct()
    {

    }

    /**
     * Return primary key condition
     *
     * @return array $condition
     *           $condition = array(
     *                 'id'    => array('S' => '10001'),
     *                 'time'  => array('N' => '1397604993'),
     *           );
     */
    protected function _getKeyConditions()
    {
        $condition = array(
            $this->_hash_key => $this->get($this->_hash_key)
        );
        if ($this->_range_key) {
            if ($this->get($this->_range_key)) {
                $condition[$this->_range_key] = $this->get($this->_range_key);
            }
        }
        $condition = $this->_formatAttributes($condition);
        return $condition;
    }

    /**
     * _formatAttributes
     *
     * @param array $array
     *
     * $array = array(
     *     'name' => 'John',
     *     'age'  => 20,
     * );
     *
     * @return array $result
     *
     * $result = array(
     *     'name' => array('S' => 'John'),
     *     'age'  => array('N' => 20),
     * );
     */
    protected function _formatAttributes($array)
    {
        $result = array();
        foreach ($array as $key => $value) {
            $type = $this->_getDataType($key);
            if ($type == 'S' || $type == 'N') {
                $value = strval($value);
            }
            $result[$key] = array($type => $value);
        }
        return $result;
    }

    /**
     * Format attribute for Update
     *
     * @param array $array
     *
     * $array = array(
     *     'name' => 'John',
     *     'age'  => 1,
     * );
     *
     * @param array $actions
     *
     * $actions = array(
     *     'count' => 'ADD', // field_name => action_name
     * );
     *
     * @return array $result
     *
     * $result = array(
     *     'name' => array(
     *         'Action' => 'PUT',
     *         'Value'  => array('S' => 'John')
     *     ),
     *     'count' => array(
     *         'Action' => 'ADD',
     *         'Value'  => array('N' => 1)
     *     ),
     * );
     */
    protected function _formatAttributeUpdates(array $array, array $actions = array())
    {
        $result = array();
        foreach ($array as $key => $value) {
            $type   = $this->_getDataType($key);
            $action = 'PUT'; // default
            if (isset($actions[$key])) {
                $action = $actions[$key]; // overwrite if $actions is set
            }
            $result[$key] = array(
                'Action' => $action,
                'Value'  => array($type => $value),
            );
        }
        return $result;
    }

    /**
     * Format attribute for Expected
     *
     * @param array $array
     *
     * $array = array(
     *     'name' => 'John',
     *     'age'  => 30,
     * );
     *
     * @param array $exists
     *
     * $exists = array(
     *     'age' => true, // field_name => bool
     * );
     *
     * @return array
     *
     * $result = array(
     *     'name' => array(
     *         'Value'  => array('S' => 'John')
     *     ),
     *     'age' => array(
     *         'Value'  => array('N' => 30),
     *         'Exists' => true
     *     )
     * );
     *
     */
    protected function _formatAttributeExpected(array $array, array $exists = array())
    {
        $result = array();
        foreach ($array as $key => $value) {
            $type         = $this->_getDataType($key);
            $result[$key] = array(
                'Value' => array($type => $value)
            );
        }
        foreach ($exists as $key => $value) {
            $result[$key]['Exists'] = $value; // set if $exists is set
        }
        return $result;
    }


    /**
     * Convert result array to simple associative array
     *
     * @param array $items
     *
     * @return array
     * @see ORM#_formatResult
     */
    protected function _formatResults(array $items)
    {
        $result = array();
        foreach ($items as $item) {
            $result[] = $this->_formatResult($item);
        }
        return $result;
    }


    /**
     * Convert result array to simple associative array
     *
     * @param array $item
     *
     * $item = array(
     *     'name'   => array('S' => 'John'),
     *     'age'    => array('N' =>  30)
     * );
     *
     * @return array $hash
     *
     * $hash = array(
     *     'name'   => 'John',
     *     'age'    => 30,
     *  );
     */
    protected function _formatResult(array $item)
    {
        $hash = array();
        foreach ($item as $key => $value) {
            $values     = array_values($value);
            $hash[$key] = $values[0];
        }
        return $hash;
    }

    /**
     * Build where conditions
     *
     * $_where_conditions = array(
     *    0 => array('name', 'EQ', 'John'),
     *    1 => array('age',  'GT', 20),
     *    2 => array('country', 'IN', array('Japan', 'Korea'))
     *  );
     *
     * @return array $result
     *
     * $result = array(
     *    'name' => array(
     *          'ComparisonOperator' => 'EQ'
     *          'AttributeValueList' => array(
     *               0 => array(
     *                       'S' => 'John'
     *                   )
     *          )
     *     ),
     *     :
     *     :
     *  );
     */
    public function _buildConditions($conditions)
    {
        $result = array();
        foreach ($conditions as $i => $condition) {
            $key      = $condition[0];
            $operator = $condition[1];
            $value    = $condition[2];

            if (!is_array($value)) {
                $value = array((string)$value);
            }

            $attributes = array();
            foreach ($value as $v) {
                $attributes[] = array($this->_getDataType($key) => (string)$v);
            }
            $result[$key] = array(
                'ComparisonOperator' => $operator,
                'AttributeValueList' => $attributes,
            );
        }
        return $result;
    }

    /**
     * Convert operator by alias
     *
     * @param  string $operator
     *
     * @return string $operator
     */
    protected function _convertOperator($operator)
    {
        $alias = array(
            '='            => 'EQ',
            '!='           => 'NE',
            '>'            => 'GT',
            '>='           => 'GE',
            '<'            => 'LT',
            '<='           => 'LE',
            '~'            => 'BETWEEN',
            '^'            => 'BEGINS_WITH',
            'NOT_NULL'     => 'NOT_NULL',
            'NULL'         => 'NULL',
            'CONTAINS'     => 'CONTAINS',
            'NOT_CONTAINS' => 'NOT_CONTAINS',
            'IN'           => 'IN',
        );
        if (isset($alias[$operator])) {
            return $alias[$operator];
        }
        if (in_array($operator, array_values($alias))) {
            return $operator;
        }
        return 'EQ'; // default
    }


    /**
     * Return data type using $_schema
     *
     * @param  string $key
     *
     * @return string $type
     *
     *          S:  String
     *          N:  Number
     *          B:  Binary
     *          SS: A set of strings
     *          NS: A set of numbers
     *          BS: A set of binary
     */
    protected function _getDataType($key)
    {
        $type = 'S';
        if (isset($this->_schema[$key])) {
            $type = $this->_schema[$key];
        }
        return $type;
    }

    /**
     * Removing all empty elements from a hash
     *
     * @param array $array
     *
     * @return array
     */
    protected function _compact(array $array)
    {
        foreach ($array as $key => $value) {
            if (is_null($value) || $value === '') {
                unset($array[$key]);
            }
        }
        return $array;
    }

    //-----------------------------------------------
    // PRIVATE METHODS (STATIC)
    //-----------------------------------------------

    // called from static factory method.
    protected static function _setupClient($connection_name = self::DEFAULT_CONNECTION)
    {
        if (!isset(self::$_client[$connection_name])) {
            $params = self::getConfig(null, $connection_name);

            if (self::getConfig('key', $connection_name) && self::getConfig('secret', $connection_name)) {
                $params['credentials'] = new Credentials(
                    self::getConfig('key', $connection_name), self::getConfig('secret', $connection_name)
                );
            }
            if (self::getConfig('base_url', $connection_name)) {
                $params['endpoint'] = self::getConfig('base_url', $connection_name);
            }

            $client                          = new DynamoDbClient($params);
            self::$_client[$connection_name] = $client;
        }
    }

    /**
     * @deprecated
     */
    protected static function _logQuery($query, array $args, $response)
    {
    }
}

class KettleException extends \Exception
{
}
