<?php

namespace Kettle;

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
    protected $_schema = [];

    /**
     * DynamoDB record data is saved here as an associative array
     *
     * @var array
     */   
    protected $_data   = [];

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
        $query = [
            $this->_hash_key => $hash_key_value,
            ];

        if ($range_key_value) {
            if (!$this->_range_key) {
                throw new \Exception("Range key is not defined.");
            }
            $query[$this->_range_key] = $range_key_value;
        }

        $result = self::$_client->get($this->_table_name, $query, $options);

        $class_name = get_called_class();
        $instance = self::factory($class_name);
        $instance->hydrate($result);
        return $instance;
    }

    /**
     * Retrive multiple results using query
     *
     */
    public function findMany($query, $options = []) {

        // query($tableName, $keyConditions, $options = array())
        $result = self::$_client->query($this->_table_name, $query, $options);

        // scan($tableName, $filter, $limit = null)
        $array  = [];
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
        $query  = $this->_getKeyConditions();
        $values = $this->_data;


        if (!$this->_is_new) {
            // remove key field when update
            foreach ($query as $key => $value) {
                unset($values[$key]);
            }
        }

        $values = $this->_formatValues($values);

        if ($this->_is_new) { // insert
            // TODO: Expected support
            $expected = [];

            // ex)
            // $values = array(
            //    "key1::S" => "value",
            //    "key2::S" => "value",
            // );
            $result = self::$_client->put($this->_table_name, $values, $expected);
        } else { // update
            // TODO: Expected support
            $expected = [];

            foreach ($values as $key => $value) {
                $values[$key] = array('PUT', $value);
            }

            // ex)
            // $values = array(
            //    "key1::S" => ["PUT", "value"],
            //    "key2::S" => ["PUT", "value"],
            // );

            $result = self::$_client->update($this->_table_name, $query, $values, $expected);
        }

        return $result;
    }

    public function delete() {
        $query  = $this->_getKeyConditions();
        $result = self::$_client->delete($this->_table_name, $query);
        return $result;
    }

    public function set($key, $value) {
        $this->_data[$key] = $value;
    } 

    public function get($key) {
        return $this->_data[$key];
    }

    public function create($data = []) {
        $this->_is_new = true;
        return $this->hydrate($data);
    }

    public function hydrate($data = []) {
        $this->_data = $data;
        return $this;
    }

    public function getClient() {
        return self::$_client;
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
        $query = [
            $this->_hash_key => $this->get($this->_hash_key)
            ];
        if ($this->_range_key) {
            if ($this->get($this->_range_key)) {
                $query[$this->_range_key] = $this->get($this->_range_key);
            }
        }
        return $query;
    }

    /**
     * Convert array format to DynamoDBWrapper format
     *
     * @params array $array
     *                  array("key1" => "value1",
     *                        "key2" => "value2"
     *                       );
     *
     * @return array $result
     *                  array("key1::S" => "value1",
     *                        "key2::N" => "value2"
     *                        );
     * 
     */
    protected function _formatValues($values) {
        $result = [];
        foreach ($values as $key => $value) {
            $type = $this->_getDataType($key);
            $result["{$key}::{$type}"] = $value;
        }
        return $result;
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
            $client = new \DynamoDBWrapper(array(
                'key'    => self::$_config['key'],
                'secret' => self::$_config['secret'],
                'region' => self::$_config['region'],
            ));
            self::$_client = $client;
        }
    }

}

