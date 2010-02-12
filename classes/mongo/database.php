<?php
/**
 * This class wraps the functionality of Mongo (connection) and MongoDB (database object) into one class.
 * When used with Kohana it can be instantiated simply by:
 * 
 *  $db = Mongo_Database::instance();
 * 
 * The above will assume the 'default' configuration from the APPPATH/config/mongo.php file.
 * Alternatively it may be instantiated with the name and configuration specified as arguments:
 * 
 *   $db = Mongo_Database::instance('test', array(
 *     'database' => 'test'
 *   ));
 *
 * The Mongo_Collection class will gain access to the server by calling the instance method with a configuration name,
 * so if not using Kohana or the configuration name is not present in the config file then the instance should be created
 * before using any classes that extend Mongo_Collection or Mongo_Document.
 *
 * Mongo_Database can proxy all methods of MongoDB to the database instance as well as select collections using the __get
 * magic method.
 *
 * If using Kohana, profiling can be enabled/disabled via the configuration or on demand by setting the profiling property.
 *
 * @method public array authenticate ( string $username , string $password )
 * @method public array command ( array $data )
 * @method public MongoCollection createCollection ( string $name [, bool $capped = FALSE [, int $size = 0 [, int $max = 0 ]]] )
 * @method public array createDBRef ( mixed $ns , mixed $a )
 * @method public array drop ( void )
 * @method public array dropCollection ( mixed $coll )
 * @method public bool forceError ( void )
 * @method public array getDBRef ( array $ref )
 * @method public MongoGridFS getGridFS ([ string $arg1 = "fs" [, string $arg2 = NULL ]] )
 * @method public int getProfilingLevel ( void )
 * @method public array lastError ( void )
 * @method public array listCollections ( void )
 * @method public array prevError ( void )
 * @method public array repair ([ bool $preserve_cloned_files = FALSE [, bool $backup_original_files = FALSE ]] )
 * @method public array resetError ( void )
 * @method public MongoCollection selectCollection ( string $name )
 * @method public int setProfilingLevel ( int $level )
 * 
 * @package Mongo_Database
 *
 * This class was adapted from http://github.com/Wouterrr/MangoDB
 */

class Mongo_Database {

	/** @static  array  Mongo_Database instances */
	public static $instances = array();

  /**
   * Get a Mongo_Database instance. Configuration options are:
   *
   *  server      A server connection string. See Mongo::__construct()
   *  options     The additional options for the connection ("connect" and "persist")
   *  database    *required* The database name to use for this instance
   *  profiling   Enable/disable profiling
   *
   * @param   string $name   The configuration name
   * @param   array $config  Pass a configuration array to bypass the Kohana config
   * @return  Mongo_Database
   * @static
   */
	public static function instance($name = 'default', array $config = NULL)
	{
		if( ! isset(self::$instances[$name]) )
		{
			if ($config === NULL)
			{
				// Load the configuration for this database
				$config = Kohana::config('mongo')->$name;
			}

			new self($name,$config);
		}

		return self::$instances[$name];
	}

	/** @var  string  Mongo_Database instance name */
	protected $_name;

	/** @var  boolean  Connection state */
	protected $_connected = FALSE;

	/** @var  Mongo  The Mongo server connection */
	protected $_connection;

	/** @var  MongoDB  The database instance for the database name chosen by the config */
	protected $_db;

  /** @var  boolean  A flag to indicate if profiling is enabled and to allow it to be enabled/disabled on the fly */
  public $profiling;

  /**
   * This cannot be called directly, use Mongo_Database::instance() instead to get an instance of this class.
   *
   * @param  string  $name  The configuration name
   * @param  array  $config The configuration data
   */
	protected function __construct($name, array $config)
	{
		$this->_name = $name;

    // Setup connection options merged over the defaults and store the connection
		$options = Arr::merge(array(
        'connect' => FALSE  // Do not connect yet
      ),
      Arr::get($config, 'options', array())
    );
    $this->_connection = new Mongo(Arr::get($config, 'server', "mongodb://".ini_get('mongo.default_host').":".ini_get('mongo.default_port')), $options);
    
    // Save the database name for later use
    $this->_db = $config['database'];
    
    // Save profiling option in a public variable
    $this->profiling = (isset($config['profiling']) && $config['profiling']);

		// Store the database instance
		self::$instances[$name] = $this;
	}

	final public function __destruct()
	{
		$this->close();
	}

  /**
   * @return  string  The configuration name
   */
	final public function __toString()
	{
		return $this->_name;
	}

  /**
   * Force the connection to be established.
   * This will automatically be called by any MongoDB methods that are proxied via __call
   * 
   * @return boolean
   * @throws MongoException
   */
	public function connect()
	{
		if( ! $this->_connected)
		{
      $this->_connected = $this->_connection->connect();
      $this->_db = $this->_connection->selectDB($this->_db);
    }
		return $this->_connected;
	}

  /**
   * Close the connection to Mongo
   */
	public function close()
	{
		if ( $this->_connection)
		{
			$this->_connection->close();
  		$this->_db = $this->_connection = NULL;
		}
    unset(self::$instances[$this->_name]);
	}

  /**
   * Proxy all methods for the MongoDB class.
   * Profiles all methods that have database interaction if profiling is enabled.
   * The database connection is established lazily.
   *
   * @param  string  $name
   * @param  array  $arguments
   * @return mixed
   */
  public function __call($name, $arguments)
  {
    $this->_connected OR $this->connect();

    if( ! method_exists($this->_db, $name))
    {
      throw new Exception("Method does not exist: MongoDb::$name");
    }

		if ( $this->profiling && ! strpos("Error",$name) && ! in_array($name, array('createDBRef','selectCollection','getGridFS')))
		{
      $json_arguments = array_map('json_encode',$arguments);
			$_bm = Profiler::start("Mongo_Database::{$this->_name}","db.$name(".implode(',',$json_arguments).")");
		}

    $retval = call_user_func_array(array($this->_db, $name), $arguments);

		if ( isset($_bm))
		{
			Profiler::stop($_bm);
		}

    return $retval;
  }

  /**
   * Same usage as MongoDB::execute except it throws an exception on error
   *
   * @param  string  $code
   * @param  array  $args
   * @return mixed
   * @throws MongoException
   */
	public function execute( $code, array $args = array() )
	{
		$retval = $this->__call('execute', array($code,$args));
    if( empty($retval['ok']) )
    {
      throw new MongoException($retval['errmsg'], $retval['errno']);
    }
    return $retval['retval'];
	}

  /**
   * Fetch a collection by using object access syntax
   *
   * @param  string  $name  The collection name to select
   * @return  MongoCollection
   */
  public function __get($name)
  {
    $this->_connected OR $this->connect();
    return $this->_db->selectCollection($name);
  }

}
