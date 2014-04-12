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

2. Create Model Class
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

3. Create
-------------------

```php
<?php

$user = ORM::factory('User')->create();
$user->id = 1;
$user->name = 'John';
$user->age  = 20;
$user->save();

```

4. Retrieve
-------------------

```php
<?php

$user = ORM::factory('User')->findOne(1);
echo $user->name. PHP_EOL;

print_r($user->asArray());

```

5. Update
-------------------

```php
<?php

$user = ORM::factory('User')->findOne(1);
$user->age = 21;
$user->save();

```

6. Delete
-------------------

```php
<?php

$user = ORM::factory('User')->findOne(1);
$user->delete();

```


7. Find
-------------------

```php
<?php

$tweets = ORM::factory('Tweets')
        ->where('user_id', 1)
        ->where('timestamp', '>', 1397264554);
        ->findMany();

foreach ($tweets as $tweet) {
     echo $tweet->text . PHP_EOL;
}

```
