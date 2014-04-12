Kettle
======

Kettle is a lightweight object-dynamodb mapper for PHP5.

See Some Code
-------------------

```php
<?php
use Kettle\ORM;

$user = ORM::factory('User')->findOne(10);
$user->name = 'John';
$user->save();


$tweets = ORM::factory('Tweet')->where('user_id', 10)
                 ->findMany();

foreach ($tweets as $tweet) {
    echo $tweet->text . PHP_EOL;
}

```

1. Setup
-------------------

```php
<?php
use Kettle\ORM;

ORM::configure("key",    'AWS_KEY');
ORM::configure("secret", 'AWS_SECRET');
ORM::configure("region", 'AWS_REGION');

```

2. Define Model Class
-------------------

```php
<?php

class User extends ORM {
    protected $_table_name = 'user';
    protected $_hash_key   = 'user_id';
    protected $_schema = array(
      'user_id'    => 'N',
      'name'       => 'S',
      'age'        => 'N',
     );
}


```

