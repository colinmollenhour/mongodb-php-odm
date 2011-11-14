<?php

defined('SYSPATH') or die('No direct script access.');

/*
 * See http://www.php.net/manual/en/mongo.construct.php for configuration options.
 */
return array(
  'default' => array(
    'database'  => 'db',
    //'server'  => 'mongodb://localhost:27017',
    'options'   => array(),
    'profiling' => FALSE
  )
);
