<?php

namespace Kettle;

use Aws\DynamoDb\DynamoDbClient;

class ORM {

    // --------------------------

    /**
     * Class configuration
     *
     * @var array
     *          - key
     *          - secret
     *          - region
     *
     */
    protected static $_config;

    // instance of DynamoDbClient class
    protected static $_client;

    // --------------------------

    // DynamoDB TableName
    protected $_table_name;

    // HashKey
    protected $_hash_key;

    // RangeKey
    protected $_range_key;

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
     * DynamoDB record data is saved here as an associative array
     *
     * @var array
     */   
    protected $_data   = array();

    protected $_data_original = array();

    // LIMIT
    protected $_limit  = null;

    /**
     * Array of WHERE clauses
     *
     * $_where_conditions = array(
     *    0 => array('name', 'EQ', 'John'),
     *    1 => array('age',  'GT', 20),
     *    2 => array('country', 'IN', array('Japan', 'Korea'))
     *  );
     */
    protected $_where_conditions = array();

    // Is this a new object (has create() been called)?
    protected $_is_new = false;

    //-----------------------------------------------
    // PUBLIC METHODS
    //-----------------------------------------------
    public function configure($key, $value) {
        self::$_config[$key] = $value;
    }

    /**
     * Retrieve single result using hash_key and range_key
     *
     * @return object  instance of the ORM sub class
     */
    public function findOne($hash_key_value, $range_key_value = null, $options = array()) {
        $query = array(
            $this->_hash_key => $hash_key_value,
        );

        if ($range_key_value) {
            if (!$this->_range_key) {
                throw new \Exception("Range key is not defined.");
            }
            $query[$this->_range_key] = $range_key_value;
        }

        $key = $this->_formatAttributes($query);
        $args = array(
            'TableName' => $this->_table_name,
            'Key'       => $key,
        );

        $item = self::$_client->getItem($args);

        if (!is_array($item['Item'])) {
            return null;
        }

        $result = $this->_formatResult($item['Item']);

        $class_name = get_called_class();
        $instance = self::factory($class_name);
        $instance->hydrate($result);
        return $instance;
    }

    /**
     * Retrieve multiple results using query
     *
     */
    public function findMany($options = array()) {

        $conditions = $this->_buildConditions();
        $result = $this->query($conditions, $options);

        // scan($tableName, $filter, $limit = null)
        $array  = array();
        $class_name = get_called_class();
        foreach ($result as $row) {
            $instance = self::factory($class_name);
            $instance->hydrate($row);
            $array[] = $instance;
        }
        return $array;
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
     *     'Action'                      => array('counter' => 'ADD'),
     *
     *     'ForceUpdate'                 => false, // If true No ConditionalCheck
     *  );
     *
     */
    public function save($options = array()) {
        $values = $this->_data;
        $expected = array();

        if ($this->_is_new) { // insert
            if (!isset($options['ForceUpdate']) || !$options['ForceUpdate']) {
                // Expect duplicate error if already exists.
                $exists   = array();
                foreach ($this->_schema as $key => $value) {
                    $exists[$key] = false;
                }
                $options['Exists'] = $exists;
            }
            $result = $this->putItem($values, $options, $expected);
        } else { // update
            if (!isset($options['ForceUpdate']) || !$options['ForceUpdate']) {
                // If data is modified by different instance or process,
                // throw Aws\DynamoDb\Exception\ConditionalCheckFailedException
                $expected = $this->_data_original;
            }
            $result = $this->updateItem($values, $options, $expected);
        }

        return $result;
    }

    /**
     * Delete record
     *
     * @return mixed
     * @see http://docs.aws.amazon.com/aws-sdk-php/latest/class-Aws.DynamoDb.DynamoDbClient.html#_deleteItem
     */
    public function delete() {
        $conditions = $this->_getKeyConditions();
        $args = array(
            'TableName'    => $this->_table_name,
            'Key'          => $conditions,
            'ReturnValues' => 'ALL_OLD',
        );

        $result = self::$_client->deleteItem($args);
        return $result;
    }

    /**
     * Add a LIMIT to the query
     *
     * @param int $limit
     */
    public function limit($limit) {
         $this->_limit = $limit;
         return $this;
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
     */
    public function where() {
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

    public function set($key, $value) {
        $this->_data[$key] = $value;
    } 

    public function get($key) {
        return isset($this->_data[$key]) ? $this->_data[$key] : null;
    }

    public function create($data = array()) {
        $this->_is_new = true;
        return $this->hydrate($data);
    }

    public function hydrate($data = array()) {
        $this->_data          = $data;
        $this->_data_original = $data;
        return $this;
    }

    /**
     * Return the raw data wrapped by this ORM instance as an associative array.
     *
     * @return array
     */
    public function asArray() {
         return $this->_data;
    }

    /**
     * Return DynamoDbClient instance
     *
     * @return object DynamoDbClient
     */
    public function getClient() {
        return self::$_client;
    }

    /**
     * query
     *
     * @param  array $conditions
     * @param  array $options
     * @return array
     *
     * @see http://docs.aws.amazon.com/aws-sdk-php/latest/class-Aws.DynamoDb.DynamoDbClient.html#_query
     */
    public function query($conditions, $options = array()) {
        $args = array(
            'TableName'        => $this->_table_name,
            'KeyConditions'    => $conditions,
            'ScanIndexForward' => true,
            'Limit'            => 100,
        );
        $result = self::$_client->query($args);
        
        return $this->_formatResults($result['Items']);
    }

    /**
     * putItem
     *
     * @param array $values
     * @param array $options
     * @param array $expected
     *
     * @see http://docs.aws.amazon.com/aws-sdk-php/latest/class-Aws.DynamoDb.DynamoDbClient.html#_putItem
     */
    public function putItem($values, $options = array(), $expected = array()) {
        $args = array(
            'TableName' => $this->_table_name,
            'Item'      => $this->_formatAttributes($values),
            //'ReturnValues'                => 'ALL_NEW',
            'ReturnConsumedCapacity'      => 'TOTAL',
            'ReturnItemCollectionMetrics' => 'SIZE',
        );

        // Set Expected if exists
        if ($expected || isset($options['Exists'])) {
            $exists = isset($options['Exists']) ? $options['Exists'] : array();
            $args['Expected'] = $this->_formatAttributeExpected($expected, $exists);
        }

        // Merge $options to $args
        $option_names = array('ReturnValues', 'ReturnConsumedCapacity', 'ReturnItemCollectionMetrics');
        foreach ($option_names as $option_name) {
            if (isset($options[$option_name])) {
                $args[$option_name] = $options[$option_name];
            }
        }

        $item = self::$_client->putItem($args);
        return $item;
    }

    /**
     * updateItem
     *
     * @param array $values associative array
     *                 $values = array(
     *                       'name' => 'John',
     *                       'age'  => 30,
     *                 );
     *
     * @param array $options
     *                $options = array(
     *                      'ReturnValues'                => 'string',
     *                      'ReturnConsumedCapacity'      => 'string',
     *                      'ReturnItemCollectionMetrics' => 'string',
     *                      'Action' => array('age' => 'ADD'),
     *                      'Exists' => array('age' => true),
     *                );
     *
     * @param array $expected
     *
     * @see http://docs.aws.amazon.com/aws-sdk-php/latest/class-Aws.DynamoDb.DynamoDbClient.html#_updateItem
     */
    public function updateItem($values, $options = array(), $expected = array()) {
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
            'TableName'        => $this->_table_name,
            'Key'              => $conditions,
            'AttributeUpdates' => $attributes,
            'ReturnValues'                => 'ALL_NEW',
            'ReturnConsumedCapacity'      => 'TOTAL',
            'ReturnItemCollectionMetrics' => 'SIZE',
        );

        // Set Expected if exists
        if ($expected || isset($options['Exists'])) {
            $exists = isset($options['Exists']) ? $options['Exists'] : array();
            $args['Expected'] = $this->_formatAttributeExpected($expected, $exists);
        }

        // Merge $options to $args
        $option_names = array('ReturnValues', 'ReturnConsumedCapacity', 'ReturnItemCollectionMetrics');
        foreach ($option_names as $option_name) {
            if (isset($options[$option_name])) {
                $args[$option_name] = $options[$option_name];
            }
        }

        $item = self::$_client->updateItem($args);
        return $item;
    }



    //-----------------------------------------------
    // MAGIC METHODS
    //-----------------------------------------------

    public function __get($key) {
        return $this->get($key);
    }

    public function __set($key, $value) {
        $this->set($key, $value);
    }

    public function __unset($key) {
        unset($this->_data[$key]);
    }

    public function __isset($key) {
        return isset($this->_data[$key]);
    }

    //-----------------------------------------------
    // PUBLIC METHODS (STATIC)
    //-----------------------------------------------
    public static function factory($class_name) {
        self::_setupClient();
        return new $class_name();
    }

    //-----------------------------------------------
    // PRIVATE METHODS
    //-----------------------------------------------
    protected function __construct() {

    }

    protected function _getKeyConditions() {
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
     *           $array = array(
     *                  'name' => 'John',
     *                  'age'  => 20,
     *           );
     *
     * @return array $result
     *           $result = array(
     *                 'name' => array('S' => 'John'),
     *                 'age'  => array('N' => 20),
     *           );
     */
    protected function _formatAttributes($array) {
        $result = array();
        foreach ($array as $key => $value) {
            $type = $this->_getDataType($key);
            $result[$key] = array($type => $value);
        }
        return $result;
    }

    /**
     *
     * @param array $array
     *              $array = array(
     *                  'name' => 'John',
     *                  'age'  => 1,
     *              );
     *
     * @param array $actions
     *              $actions = array(
     *                  'count' => 'ADD', // field_name => action_name
     *              );
     *
     * @return array $result
     *               $result = array(
     *                  'name' => array(
     *                       'Action' => 'PUT',
     *                       'Value'  => array('S' => 'John')
     *                  ),
     *                  'count' => array(
     *                       'Action' => 'ADD',
     *                       'Value'  => array('N' => 1)
     *                  ),
     *              );
     */
    protected function _formatAttributeUpdates($array, $actions = array()) {
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

    protected function _formatAttributeExpected($array, $exists = array()) {
        $result = array();
        foreach ($array as $key => $value) {
            $type   = $this->_getDataType($key);
            $result[$key] = array(
                'Value'  => array($type => $value)
            );
        }
        foreach ($exists as $key => $value) {
            $result[$key]['Exists'] = $value; // set if $exists is set
        }
        return $result;
    }


    protected function _formatResults($items) {
        $result = array();
        foreach ($items as $item) {
            $result[] = $this->_formatResult($item);
        }
        return $result;
    }

    protected function _formatResult($item) {
        $hash = array();
        foreach ($item as $key => $value) {
            $values = array_values($value);
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
    public function _buildConditions() {
        $result     = array();
        $conditions = $this->_where_conditions;
        foreach ($conditions as $i => $condition) {
            $key      = $condition[0];
            $operator = $condition[1];
            $value    = $condition[2];

            if (!is_array($value)) {
                $value = array((string) $value);
            }

            $attributes = array();
            foreach ($value as $v) {
                $attributes[] = array($this->_getDataType($key) => (string) $v);
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
     * @return string $operator
     */
    protected function _convertOperator($operator) {
        $alias = array(
            '='  => 'EQ',
            '!=' => 'NE',
            '>'  => 'GT',
            '>=' => 'GE',
            '<'  => 'LT',
            '<=' => 'LE',
            '~'  => 'BETWEEN',
            '^'  => 'BEGINS_WITH',
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
     * @return string $type
     *
     *          S:  String
     *          N:  Number
     *          B:  Binary
     *          SS: A set of strings
     *          NS: A set of numbers
     *          BS: A set of binary
     */
    protected function _getDataType($key) {
        $type = 'S';
        if (isset($this->_schema[$key])) {
            $type = $this->_schema[$key];
        }
        return $type;
    }

    //-----------------------------------------------
    // PRIVATE METHODS (STATIC)
    //-----------------------------------------------

    // called from static factory method.
    protected static function _setupClient() {
        if (!self::$_client) {
            $params = array(
                'key'    => self::$_config['key'],
                'secret' => self::$_config['secret'],
                'region' => self::$_config['region'],
            );
            $client = DynamoDbClient::factory($params);
            self::$_client = $client;
        }
    }

}

