#!/usr/bin/env php
<?php
/**
 * ORM Skeleton generator script
 *
 * Usage:
 *
 *   php vendor/bin/kettle-skeleton.php --table-name TABLE_NAME --region ap-northeast-1 > TableName.php
 *
 */

$files = [
    __DIR__ . '/../vendor/autoload.php',
    __DIR__ . '/../../../autoload.php',
];

foreach ($files as $file) {
    if (file_exists($file)) {
        require $file;
        break;
    }
}

use Aws\DynamoDb\DynamoDbClient;

/**
 * main
 *
 * @void
 */
function main()
{
    $args = get_args();

    if (!isset($args['table-name'])) {
        echo "ERROR: argument 'table-name' is required.\n\n";
        print_usage();
        die();
    }
    $region = 'us-west-2';
    if (isset($args['region'])) {
        $region = $args['region'];
    }

    $table_name = $args['table-name'];

    $params = [
        'version' => '2012-08-10',
        'region'  => $region,
    ];

    $client = new DynamoDbClient($params);

    // Table Descriptions
    try {
        $result = $client->describeTable(['TableName' => $table_name]);
    } catch (\Exception $e) {
        echo "DynamoDB ERROR: " . $e->getMessage() . "\n\n";
        print_usage();
        die();
    }

    // AttributeDefinitions
    $attribute_definitions = $result->search('Table.AttributeDefinitions[]');

    // KeySchema
    $key_schema = $result->search('Table.KeySchema[]');

    // GSI
    $global_secondary_indexes = $result->search('Table.GlobalSecondaryIndexes[]');

    // LSI
    $local_secondary_indexes = $result->search('Table.LocalSecondaryIndexes[]');

    // Items
    $result_items = $client->scan(['TableName' => $table_name, 'Limit' => 10]);
    $items        = $result_items->search("Items[]");

    $template = <<<'EOT'
<?php

//require __DIR__ . '/vendor/autoload.php';

use Kettle\ORM;

class %CLASS_NAME% extends ORM
{
    protected $_table_name = '%TABLE_NAME%';
    protected $_hash_key   = '%HASH_KEY%';
    //protected $_range_key  = '%RANGE_KEY%';
    //protected $_global_secondary_indexes = %GLOBAL_SECONDARY_INDEXES%;
    //protected $_local_secondary_indexes = %LOCAL_SECONDARY_INDEXES%;

    protected $_schema = %SCHEMA%;

}

EOT;

    $replace = [];

    //---------------------------------------
    // HASH & RANGE
    //---------------------------------------
    $hash_key  = '';
    $range_key = '';
    foreach ($key_schema as $row) {
        if ($row['KeyType'] == 'HASH') {
            $hash_key = $row['AttributeName'];
        }
        if ($row['KeyType'] == 'RANGE') {
            $range_key = $row['AttributeName'];
        }
    }

    $replace['%HASH_KEY%']  = $hash_key;
    $replace['%RANGE_KEY%'] = $range_key;
    if ($range_key) {
        $replace['//protected $_range_key'] = 'protected $_range_key';
    }

    //---------------------------------------
    // TABLE NAME
    //---------------------------------------
    $replace['%TABLE_NAME%'] = $table_name;

    //---------------------------------------
    // CLASS NAME
    //---------------------------------------

    $class_name              = table_name_to_class_name($table_name);
    $replace['%CLASS_NAME%'] = $class_name;

    //---------------------------------------
    // SCHEMA
    //---------------------------------------

    $schema = [];

    // From sample items
    foreach ($items as $item) {
        foreach ($item as $attribute_name => $type_value) {
            foreach ($type_value as $type => $value) {
                $schema[$attribute_name] = $type;
            }
        }
    }

    // From attribute definitions
    foreach ($attribute_definitions as $row) {
        $attribute_name          = $row['AttributeName'];
        $attribute_type          = $row['AttributeType'];
        $schema[$attribute_name] = $attribute_type;
    }

    // max length
    $max_attribute_len = 0;
    foreach ($schema as $k => $v) {
        if (strlen($k) > $max_attribute_len) {
            $max_attribute_len = strlen($k);
        }
    }
    ksort($schema);

    $schema_code = "[\n";
    foreach ($schema as $key => $val) {
        $key = str_pad("'{$key}'", $max_attribute_len + 2, " ", STR_PAD_RIGHT);
        $schema_code .= "        {$key} => '{$val}',\n";
    }
    $schema_code .= "    ]";

    $replace['%SCHEMA%'] = $schema_code;

    //---------------------------------------
    // GSI
    //---------------------------------------

    $code = build_index_code($global_secondary_indexes);
    if ($global_secondary_indexes) {
        $replace['//protected $_global_secondary_indexes'] = 'protected $_global_secondary_indexes';
    }
    $replace['%GLOBAL_SECONDARY_INDEXES%'] = $code;

    //---------------------------------------
    // LSI
    //---------------------------------------

    $code = build_index_code($local_secondary_indexes);
    if ($local_secondary_indexes) {
        $replace['//protected $_local_secondary_indexes'] = 'protected $_local_secondary_indexes';
    }
    $replace['%LOCAL_SECONDARY_INDEXES%'] = $code;


    $result = build_skeleton_code($template, $replace);
    echo $result;
}

/**
 * Convert table_name to ClassName
 *
 * @param string $table_name
 *
 * @return string
 */
function table_name_to_class_name($table_name)
{
    $table_name = ucwords(str_replace('_', ' ', $table_name));
    $table_name = str_replace(' ', '', $table_name);
    return $table_name;
}

/**
 * Build index code
 *
 * @param array|null $indexes
 *
 * @return string
 */
function build_index_code($indexes)
{
    if (!$indexes) {
        return '[]';
    }

    $code = "[\n";
    foreach ($indexes as $index) {
        $code .= "        '{$index['IndexName']}' => [";
        $_keys = [];
        foreach ($index['KeySchema'] as $row) {
            if ($row['KeyType'] == 'HASH') {
                $_keys[0] = $row['AttributeName'];
            }
            if ($row['KeyType'] == 'RANGE') {
                $_keys[1] = $row['AttributeName'];
            }
        }
        $code .= "'" . join("','", $_keys) . "'";
        $code .= "],\n";
    }
    $code .= "    ]";

    return $code;
}

/**
 * Build skeleton code
 *
 * @param string $template
 * @param array  $replace
 *
 * @return string
 */
function build_skeleton_code($template, array $replace)
{
    foreach ($replace as $key => $value) {
        $template = str_replace($key, $value, $template);
    }
    return $template;
}


function get_args()
{
    global $argv;

    $args        = [];
    $current_key = null;
    for ($i = 1, $total = count($argv); $i < $total; $i++) {
        if ($i % 2) {
            if (substr($argv[$i], 0, 2) == '--') {
                $current_key = str_replace('--', '', $argv[$i]);
            } else {
                $current_key = trim($argv[$i]);
            }
        } else {
            $args[$current_key] = $argv[$i];
            $current_key        = null;
        }
    }
    return $args;
}

function print_usage()
{
    $usage = <<<'EOT'
Usage:
    bin/kettle-skeleton.php --table-name TABLE_NAME --region ap-northeast-1

EOT;
    echo $usage;
}

main();
