<?php defined('SYSPATH') or die('No direct script access.');
/*
 * See http://www.php.net/manual/en/mongo.construct.php for configuration options.
 */
return array(
    Kohana::DEVELOPMENT => array(
        'database'  => '40_db_v1',
		'server'	=> 'mongodb://localhost:27017',
        'options'   => array(),
        'profiling' => TRUE,
    ),
    Kohana::STAGING => array(
        'database'  => '40_db_v1',
		'server'	=> 'mongodb://localhost:27017',
        'options'   => array(),
        'profiling' => TRUE,
    ),
    'default' => array(
        'database'  => 'db_v1',
		'server'	=> 'mongodb://localhost:27017',
        'options'   => array(),
        'profiling' => TRUE,
    ),
);
