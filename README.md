Kettle
======

Kettle is a lightweight object-dynamodb mapper for PHP.
Kettle provides a simple interface to Amazon DynamoDB.

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

1. Installation
-------------------

Package is available on Packagist, you can install it using Composer.

```
$ cat <<EOF > composer.json
{
    "require": {
        "kettle/dynamodb-orm": "0.2.0"
    }
}
EOF

$ composer install
```


2. Configuration
-------------------

```php
<?php
use Kettle\ORM;

ORM::configure("key",    'AWS_KEY');
ORM::configure("secret", 'AWS_SECRET');
ORM::configure("region", 'AWS_REGION');

// In order to use DynamoDB Local, you need to set "base_url".
// ORM::configure("base_url", 'http://localhost:8000/');

```

If you are using multiple aws account, use as follows.

```php
<?php
use Kettle\ORM;

ORM::configure("key",    'AWS_KEY',    'account-2');
ORM::configure("secret", 'AWS_SECRET', 'account-2');
ORM::configure("region", 'AWS_REGION', 'account-2');

$user = ORM::factory('User', 'account-2');

```

3. Create Model Class
-------------------

```php
<?php

class User extends ORM {
    protected $_table_name = 'user';
    protected $_hash_key   = 'user_id';
    protected $_schema = array(
      'user_id'    => 'N',  // user_id is number
      'name'       => 'S',  // name is string
      'age'        => 'N',
      'country'    => 'S',
     );
}


```

4. Create
-------------------

```php
<?php

$user = ORM::factory('User')->create();
$user->user_id = 1;
$user->name    = 'John';
$user->age     = 20;
$user->save();

```

5. Retrieve
-------------------

```php
<?php

$user = ORM::factory('User')->findOne(1);
echo $user->name. PHP_EOL;

print_r($user->asArray());

```

6. Update
-------------------

```php
<?php

$user = ORM::factory('User')->findOne(1);
$user->age = 21;
$user->save();

```

7. Delete
-------------------

```php
<?php

$user = ORM::factory('User')->findOne(1);
$user->delete();

```


8. Find
-------------------

```php
<?php

$tweets = ORM::factory('Tweets')
        ->where('user_id', 1)
        ->where('timestamp', '>', 1397264554)
        ->findMany();

foreach ($tweets as $tweet) {
     echo $tweet->text . PHP_EOL;
}

```

9. Find first record
-------------------

```php
<?php

$tweet = ORM::factory('Tweets')
        ->where('user_id', 1)
        ->where('timestamp', '>', 1397264554)
        ->findFirst();

echo $tweet->text . PHP_EOL;

```


10. Find by Global Secondary Index
-------------------

```php
<?php

$users = ORM::factory('User')
        ->where('country', 'Japan')
        ->where('age', '>=', 20)
        ->index('country-age-index')  // specify index name
        ->findMany();

```


11. Query Filtering
-------------------

```php
<?php

$tweets = ORM::factory('Tweets')
          ->where('user_id', 1)
          ->filter('is_deleted', 0) // using filter
          ->findMany();

```

