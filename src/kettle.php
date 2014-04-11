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

    // instance of DynamoDBWrapper class
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

    // Is this a new object (has create() been called)?
    protected $_is_new = false;

    //-----------------------------------------------
    // PUBLIC METHODS
    //-----------------------------------------------
    public function configure($key, $value) {
        self::$_config[$key] = $value;
    }

    /**
     * Retrive single result using hash_key and range_key
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
     * Retrive multiple results using query
     *
     */
    public function findMany($query, $options = array()) {

        // query($tableName, $keyConditions, $options = array())
        $result = $this->query($query, $options);

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
     */
    public function save() {
        $values = $this->_data;

        if ($this->_is_new) { // insert
            // TODO: Expected support
            $expected = array();
            $result = $this->putItem($values, $expected);
        } else { // update
            // TODO: Expected support
            $expected = array();
            $result = $this->updateItem($values, $expected);
        }

        return $result;
    }

    // @see http://docs.aws.amazon.com/aws-sdk-php/latest/class-Aws.DynamoDb.DynamoDbClient.html#_deleteItem
    public function delete() {
        $conditions = $this->_getKeyConditions();
        $conditions = $this->_formatAttributes($conditions);
        $args = array(
            'TableName' => $this->_table_name,
            'Key' => $conditions,
            'ReturnValues' => 'ALL_OLD',
        );

        $result = self::$_client->deleteItem($args);
        return $result;
    }

    public function set($key, $value) {
        $this->_data[$key] = $value;
    } 

    public function get($key) {
        return $this->_data[$key];
    }

    public function create($data = array()) {
        $this->_is_new = true;
        return $this->hydrate($data);
    }

    public function hydrate($data = array()) {
        $this->_data = $data;
        return $this;
    }

    public function getClient() {
        return self::$_client;
    }

    public function query($query, $options = array()) {
        $args = array(
            'TableName' => $this->_table_name,
            'KeyConditions' => $this->_formatConditions($query),
            'ScanIndexForward' => true,
            'Limit' => 100,
        );
        $result = self::$_client->query($args);
        
        return $this->_formatResults($result['Items']);
    }

    public function putItem($values, $expected = array()) {
        $args = array(
            'TableName' => $this->_table_name,
            'Item'      => $this->_formatAttributes($values),
        );
        if (!empty($expected)) {
            $args['Expected'] = $expected;
        }
        $item = self::$_client->putItem($args);
    }

    /**
     * @see http://docs.aws.amazon.com/aws-sdk-php/latest/class-Aws.DynamoDb.DynamoDbClient.html#_updateItem
     */
    public function updateItem($values) {
        $conditions = $this->_getKeyConditions();
        $conditions = $this->_formatAttributes($conditions);
        $attrs      = $this->_formatAttributeUpdates($values);

        foreach ($conditions as $key => $value) {
            if (isset($attrs[$key])) {
               unset($attrs[$key]);
            }
        }
        $args = array(
            'TableName'        => $this->_table_name,
            'Key'              => $conditions,
            'AttributeUpdates' => $attrs,
        );
        $item = self::$_client->updateItem($args);
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

        $condition = array();

        $query = array(
            $this->_hash_key => $this->get($this->_hash_key)
        );
        if ($this->_range_key) {
            if ($this->get($this->_range_key)) {
                $query[$this->_range_key] = $this->get($this->_range_key);
            }
        }
        return $query;
    }

    protected function _formatAttributes($array) {
        $result = array();
        foreach ($array as $key => $value) {
            $type = $this->_getDataType($key);
            $result[$key] = array($type => $value);
        }
        return $result;
    }

    protected function _formatAttributeUpdates($array) {
        $result = array();
        foreach ($array as $key => $value) {
            $type = $this->_getDataType($key);
            $action = 'PUT'; // TODO
            $result[$key] = array(
                'Action' => $action,
                'Value' => array($type => $value),
            );
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
     * $input_condition = array(
     *    'key1' => 'value',
     *    'key2' => array('=', 10),
     *    'key3' => array('!=', 10),
     *    'key4' => array('>', 10),
     *    'key5' => array('>=', 10),
     *    'key6' => array('<', 10),
     *    'key7' => array('<=', 10),
     *    'key8' => array('~', 10, 11),
     * );
    */
    protected function _formatConditions($conditions) {
        $result = array();
        foreach ($conditions as $key => $value) {
            if (!is_array($value)) {
                $value = array('=', (string) $value);
            }
            $operator = array_shift($value);
            $operator = $this->_convertOperator($operator);
            //$value    = count($_value) > 1 ? $_value[1] : null;

            $attrs = array();
            foreach ($value as $v) {
                $attrs[] = array($this->_getDataType($key) => (string) $v);
            }
            $result[$key] = array(
                'ComparisonOperator' => $operator,
                'AttributeValueList' => $attrs,
            );
        }
        return $result;
    }

    protected function _convertOperator($operator) {
        $operator_map = array(
            '='  => 'EQ',
            '!=' => 'NE',
            '>'  => 'GT',
            '>=' => 'GE',
            '<'  => 'LT',
            '<=' => 'LE',
            '~'  => 'BETWEEN',
            'IN' => 'IN',
            );
        if (isset($operator_map[$operator])) {
            return $operator_map[$operator];
        }
        if (in_array($operator, array_values($operator_map))) {
            return $operator;
        }
        return 'EQ';
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

