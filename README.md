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
