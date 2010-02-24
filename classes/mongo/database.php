<?php
/**
 * This class wraps the functionality of Mongo (connection) and MongoDB (database object) into one class.
 * When used with Kohana it can be instantiated simply by:
 *
 * <code>
 *  $db = Mongo_Database::instance();
 * </code>
 * 
 * The above will assume the 'default' configuration from the APPPATH/config/mongo.php file.
 * Alternatively it may be instantiated with the name and configuration specified as arguments:
 *
 * <code>
 *   $db = Mongo_Database::instance('test', array(
 *     'database' => 'test'
 *   ));
 * </code>
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
 * @method array authenticate()  authenticate( string $username , string $password )
 * @method array command()  command( array $data )
 * @method MongoCollection createCollection()  createCollection( string $name [, bool $capped = FALSE [, int $size = 0 [, int $max = 0 ]]] )
 * @method array createDBRef()  createDBRef( mixed $ns , mixed $a )
 * @method array drop()  drop( void )
 * @method array dropCollection()  dropCollection( mixed $coll )
 * @method bool forceError()  forceError( void )
 * @method array getDBRef()  getDBRef( array $ref )
 * @method MongoGridFS getGridFS()  getGridFS([ string $arg1 = "fs" [, string $arg2 = NULL ]] )
 * @method int getProfilingLevel()  getProfilingLevel( void )
 * @method array lastError()  lastError( void )
 * @method array listCollections()  listCollections( void )
 * @method array prevError()  prevError( void )
 * @method array repair()  repair([ bool $preserve_cloned_files = FALSE [, bool $backup_original_files = FALSE ]] )
 * @method array resetError()  resetError( void )
 * @method MongoCollection selectCollection()  selectCollection( string $name )
 * @method int setProfilingLevel()  setProfilingLevel( int $level )
 *
 * @author  Colin Mollenhour
 * @package Mongo_Database
 *
 * This class was adapted from http://github.com/Wouterrr/MangoDB
 */

class Mongo_Database {

	/** Mongo_Database instances
   *  @static  array */
	protected static $instances = array();

  /**
   * Get a Mongo_Database instance. Configuration options are:
   *
   * <pre>
   *  server      A server connection string. See Mongo::__construct()
   *  options     The additional options for the connection ("connect" and "persist")
   *  database    *required* The database name to use for this instance
   *  profiling   Enable/disable profiling
   * </pre>
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

	/** Mongo_Database instance name
   *  @var  string */
	protected $_name;

	/** Connection state
   *  @var  boolean */
	protected $_connected = FALSE;

	/** The Mongo server connection
   *  @var  Mongo */
	protected $_connection;

	/** The database instance for the database name chosen by the config
   *  @var  MongoDB */
	protected $_db;

  /** A flag to indicate if profiling is enabled and to allow it to be enabled/disabled on the fly
   *  @var  boolean */
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
   * Expose the MongoDb instance directly.
   *
   * @return  MongoDb
   */
  public function db()
  {
    $this->_connected OR $this->connect();
    return $this->_db;
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

		if ( $this->profiling && ! strpos("Error",$name) && $name != 'createDBRef' )
		{
      $json_arguments = array(); foreach($arguments as $arg) $json_arguments[] = json_encode((is_array($arg) ? (object)$arg : $arg));
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
   * Get a Mongo_Collection instance (wraps MongoCollection)
   * 
   * @param  string  $name
   * @return Mongo_Collection
   */
  public function selectCollection($name)
  {
    $this->_connected OR $this->connect();
    return new Mongo_Collection($name, $this->_name);
  }

  /**
   * Get a Mongo_Collection instance with grid FS enabled (wraps MongoCollection)
   *
   * @param  string  $prefix
   * @return Mongo_Collection
   */
  public function getGridFS($prefix = 'fs')
  {
    $this->_connected OR $this->connect();
    return new Mongo_Collection($prefix, $this->_name, TRUE);
  }

  /**
   * Fetch a collection by using object access syntax
   *
   * @param  string  $name  The collection name to select
   * @return  Mongo_Collection
   */
  public function __get($name)
  {
    return $this->selectCollection($name);
  }

}
