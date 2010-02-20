<?php
/**
 * This class works closely with Mongo_Document. When extended you must override at least the $name variable
 * with the collection name managed by this class.
 *
 * @author  Colin Mollenhour
 * @package Mongo_Database
 */

class Mongo_Collection implements Iterator, Countable {

  const ASC = 1;
  const DESC = -1;

  /**
   * Instantiate an object conforming to Mongo_Collection conventions.
   *
   * @param   string  $name The model name to instantiate
   * @return  Mongo_Collection
   */
  public static function factory($name)
  {
    $class = 'Model_'.$name.'_Collection';
    return new $class;
  }

  /** The name of the collection within the database or the gridFS prefix if gridFS is TRUE
   *  @var  string */
  protected $name;

  /** The database configuration name (passed to Mongo_Database::instance() )
   *  @var  string  */
  protected $db = 'default';

  /** Whether or not this collection is a gridFS collection
   *  @var  boolean */
  protected $gridFS = FALSE;

  /** Indicates if the Collection was instantiated for direct access or ODM access
   *  @var  boolean */
  protected $_direct = FALSE;

  /** The class name of the corresponding document model (cached)
   *  @var  string */
  protected $_model; // Defaults to class name without _Collection

  /** An instance of the corresponding document model
   *  @var  Mongo_Document */
  protected $_model_object;

  /** The cursor instance in use while iterating a collection
   *  @var  MongoCursor */
  protected $_cursor;

  /** The current query criteria (with field names translated)
   *  @var  array */
  protected $_query = array();

  /** The current query fields (a hash of 'field' => 1)
   *  @var  array */
  protected $_fields = array();

  /** The current query options
   *  @var  array */
  protected $_options = array();

  /** A cache of MongoCollection instances for performance
   *  @static  array */
  protected static $collections = array();

  /**
   * Instantiate a new collection object, can be used for querying, updating, etc..
   * 
   * @param  string  $name  The collection name
   * @param  string  $db    The database configuration name
   * @param  boolean $gridFS  Is the collection a gridFS instance?
   */
  public function __construct($name = NULL, $db = 'default', $gridFS = FALSE)
  {
    if($name !== NULL)
    {
      $this->db = $db;
      $this->name = $name;
      $this->gridFS = $gridFS;
      $this->_direct = TRUE;
    }
  }

  /**
   * Reset the state of the query (must be called manually if re-using a collection for a new query)
   *
   * @return  Mongo_Collection
   */
  public function reset()
  {
    $this->_query = $this->_fields = $this->_options = array();
    $this->_cursor = NULL;
    return $this;
  }

  /**
   * Magic method override passes on method calls to either the MongoCursor or the MongoCollection
   *
   * @param   string $name
   * @param   array $arguments
   * @return  mixed
   */
  public function __call($name, $arguments)
  {
    if($this->_cursor && method_exists($this->_cursor, $name))
    {
      return call_user_func_array(array($this->_cursor, $name), $arguments);
    }
 
    if(method_exists($this->collection(),$name))
    {
      if($this->db()->profiling && in_array($name,array('batchInsert','findOne','getDBRef','group','insert','remove','save','update')))
      {
        $json_arguments = array_map('json_encode',$arguments);
        $bm = Profiler::start("Mongo_Database::$this->db","db.$this->name.$name(".implode(',',$json_arguments).")");
      }

      $return = call_user_func_array(array($this->collection(), $name), $arguments);

      if(isset($bm))
      {
        Profiler::stop($bm);
      }

      return $return;
    }

    trigger_error('Method not found by Mongo_Collection: '.$name);
  }

  /**
   * Get the Mongo_Database instance used for this collection
   *
   * @return  Mongo_Database
   */
  public function db()
  {
    return Mongo_Database::instance($this->db);
  }

  /**
   * Get the corresponding MongoCollection instance
   *
   * @param  boolean  $fresh  Pass TRUE if you don't want to re-use the cached instance
   * @return  MongoCollection
   */
  public function collection($fresh = FALSE)
  {
    if($fresh === TRUE)
    {
      $selectMethod = ($this->gridFS ? 'getGridFS' : 'selectCollection');
      return $this->db()->db()->$selectMethod($this->name);
    }
    
    if( ! isset(self::$collections[$this->name]))
    {
      $selectMethod = ($this->gridFS ? 'getGridFS' : 'selectCollection');
      self::$collections[$this->name] = $this->db()->db()->$selectMethod($this->name);
    }
    return self::$collections[$this->name];
  }

  /**
   * Set some criteria for the query. Unlike MongoCollection::find, this can be called multiple
   * times and the query paramters will be merged together.
   *
   * <pre>
   * Usages:
   *   $query is an array
   *   $query is a field name and $value is the value to search for
   *   $query is a JSON string that will be interpreted as the query criteria
   * </pre>
   *
   * @param   mixed $query  An array of paramters or a key
   * @param   mixed $value  If $query is a key, this is the value
   * @return  Mongo_Collection
   */
  public function find($query = array(), $value = NULL)
  {
    if($this->_cursor) throw new MongoCursorException('The cursor has already started iterating.');
    if(is_string($query))
    {
      if($query[0] == "{")
      {
        $query = JSON::arr($query);
        if($query === NULL)
        {
          throw new Exception('Unable to parse query from JSON string.');
        }
      }
      else
      {
        $query = array($query => $value);
      }
    }

    // Translate field aliases
    $query_fields = array();
    foreach($query as $field => $value)
    {
      $query_fields[$this->get_field_name($field)] = $value;
    }

    $this->_query = Arr::merge($this->_query, $query_fields);
    return $this;
  }

  /**
   * Add fields to be returned by the query.
   *
   * @param   array $fields
   * @return  Mongo_collection
   */
  public function fields($fields = array())
  {
    if($this->_cursor) throw new MongoCursorException('The cursor has already started iterating.');

    // Translate field aliases
    foreach($fields as $field)
    {
      $this->_fields[$this->get_field_name($field)] = 1;
    }

    return $this;
  }

  /**
   * Gives the database a hint about the query
   *
   * @param   array
   * @return  Mongo_Collection
   */
  public function hint(array $key_pattern)
  {
    if($this->_cursor) throw new MongoCursorException('The cursor has already started iterating.');
    $this->_options['hint'] = $key_pattern;
    return $this;
  }

  /**
   * Sets whether this cursor will timeout
   *
   * @param   boolean
   * @return  Mongo_Collection
   */
  public function immortal($liveForever = TRUE)
  {
    if($this->_cursor) throw new MongoCursorException('The cursor has already started iterating.');
    $this->_options['immortal'] = $liveForever;
    return $this;
  }

  /**
   * Limits the number of results returned
   *
   * @param   int
   * @return  Mongo_Collection
   */
  public function limit($num)
  {
    if($this->_cursor) throw new MongoCursorException('The cursor has already started iterating.');
    $this->_options['limit'] = $num;
    return $this;
  }

  /**
   * Skips a number of results
   *
   * @param   int
   * @return  Mongo_Collection
   */
  public function skip($num)
  {
    if($this->_cursor) throw new MongoCursorException('The cursor has already started iterating.');
    $this->_options['skip'] = $num;
    return $this;
  }

  /**
   * Sets whether this query can be done on a slave
   *
   * @param   boolean
   * @return  Mongo_Collection
   */
  public function slaveOkay($okay = TRUE)
  {
    if($this->_cursor) throw new MongoCursorException('The cursor has already started iterating.');
    $this->_options['slaveOkay'] = $okay;
    return $this;
  }

  /**
   * Use snapshot mode for the query
   *
   * @return  Mongo_Collection
   */
  public function snapshot()
  {
    if($this->_cursor) throw new MongoCursorException('The cursor has already started iterating.');
    $this->_options['snapshot'] = NULL;
    return $this;
  }

  /**
   * Sorts the results by given fields
   *
   * @param   mixed  A sort criteria or a key (requires corresponding $value)
   * @param   mixed  The direction if $fields is a key
   * @return  Mongo_Collection
   */
  public function sort($fields, $direction = 1)
  {
    if($this->_cursor) throw new MongoCursorException('The cursor has already started iterating.');

    if( ! isset($this->_options['sort']))
    {
      $this->_options['sort'] = array();
    }

    if( ! is_array($fields))
    {
      $fields = array($fields => $direction);
    }

    // Translate field aliases
    foreach($fields as $field => $direction)
    {
      $this->_options['sort'][$this->get_field_name($field)] = $direction;
    }

    return $this;
  }

  /**
   * Sorts the results ascending by the given field
   *
   * @param   string  The field name to sort by
   * @return  Mongo_Collection
   */
  public function sort_asc($field)
  {
    return $this->sort($field,self::ASC);
  }

  /**
   * Sorts the results descending by the given field
   *
   * @param   string  The field name to sort by
   * @return  Mongo_Collection
   */
  public function sort_desc($field)
  {
    return $this->sort($field,self::DESC);
  }

  /**
   * Sets whether this cursor will be left open after fetching the last results
   *
   * @param   boolean
   * @return  Mongo_Collection
   */
  public function tailable($tail = TRUE)
  {
    if($this->_cursor) throw new MongoCursorException('The cursor has already started iterating.');
    $this->_options['tailable'] = $tail;
    return $this;
  }

  /**
   * Force the collection to be loaded, after this is called the query cannot be modified.
   * This is automatically called when the iterator initializes (rewind).
   *
   * @return  Mongo_Collection
   */
  public function load($skipBenchmark = FALSE)
  {
    // Execute the query and set the options
    $this->_cursor = $this->collection()->find($this->_query, array_keys($this->_fields));
    foreach($this->_options as $key => $value)
    {
      if($value === NULL) $this->_cursor->$key();
      else $this->_cursor->$key($value);
    }

    if($this->db()->profiling && ! $skipBenchmark)
    {
      $this->_bm = Profiler::start("Mongo_Database::$this->db",$this->__toString());
    }

    return $this;
  }

  /**
   * Get an instance of the corresponding document model.
   *
   * @return  Mongo_Document
   */
  public function get_model()
  {
    if($this->_direct)
    {
      throw new Exception('Cannot call get_model on a Mongo_Collection instance that was instantiated directly.');
    }

    if( ! $this->_model_object)
    {
      if( ! $this->_model)
      {
        $this->_model = substr(get_class($this),0,-11);
      }
      $model = $this->_model;
      $this->_model_object = new $model;
    }
    return $this->_model_object;
  }

  /**
   * Translate a field name according to aliases defined in the model if they exist.
   *
   * @param  string $name
   * @return string
   */
  public function get_field_name($name)
  {
    if($this->_direct)
    {
      return $name;
    }
    return $this->get_model()->get_field_name($name);
  }

  /**
   * Access the MongoCursor instance directly, triggers a load if there is none.
   *
   * @return  MongoCursor
   */
  public function cursor()
  {
    if( ! $this->_cursor)
    {
      $this->load();
    }
    return $this->_cursor;
  }

  /**
   * Returns the current query results as an array
   *
   * @param   boolean $objects  Pass FALSE to get raw data
   * @return  array
   */
  public function as_array( $objects = TRUE )
  {
    $array = array();

    if($objects)
    {
      foreach($this as $key => $value)
      {
        $array[$key] = $value;
      }
    }

    else
    {
      foreach($this->_cursor as $key => $value)
      {
        $array[$key] = $value;
      }
    }

    return $array;
  }

  /**
   * Return an array of values or an associative array of keys and values
   *
   * @param   string $key
   * @param   mixed $val
   * @return  array
   */
  public function select_list($key = '_id',$val = NULL)
  {
    if($val === NULL)
    {
      $val = $key;
      $key = NULL;
    }

    $list = array();

    foreach($this->_cursor as $data)
    {
      if($key !== NULL)
      {
        $list[(string) $data[$key]] = (isset($data[$val]) ? $data[$val] : NULL);
      }
      else if(isset($data[$val]))
      {
        $list[] = $data[$val];
      }
    }

    return $list;
  }

  /********************************
   * Iterator and Countable methods
   ********************************/

  /**
   * Countable: count
   *
   * Count the results from the current query: pass FALSE for "all" results (disregard limit/skip)<br/>
   * Count results of a separate query: pass an array or JSON string of query parameters
   *
   * @param  mixed $query
   * @return int
   */
  public function count($query = TRUE)
  {
    if(is_bool($query))
    {
      // Profile count operation for cursor
      if($this->db()->profiling)
      {
        $bm = Profiler::start("Mongo_Database::$this->db","$this.count(".JSON::str($query).")");
      }

      if( ! $this->_cursor)
      {
        $this->load(TRUE);
      }

      $count = $this->_cursor->count($query);
    }
    else
    {
      if(is_string($query) && $query[0] == "{")
      {
        $query = JSON::arr($query);
        if($query === NULL)
        {
          throw new Exception('Unable to parse query from JSON string.');
        }
      }
      // Profile count operation for collection
      if($this->db()->profiling)
      {
        $bm = Profiler::start("Mongo_Database::$this->db","db.$this->name.count(".($query ? JSON::str($query):'').")");
      }

      $count = $this->collection()->count($query);
    }

    // End profiling count
    if(isset($bm))
    {
      Profiler::stop($bm);
    }

    return $count;
  }

  /**
   * Implement MongoCursor#getNext so that the return value is a Mongo_Document instead of array
   *
   * @return  Mongo_Document
   */
  public function getNext()
  {
    $this->_cursor->next();
    return $this->current();
  }

  /**
   * Iterator: current
   */
  public function current()
  {
    $data = $this->_cursor->current();

    if(isset($this->_bm))
    {
      Profiler::stop($this->_bm);
      unset($this->_bm);
    }

    if($this->_direct)
    {
      return $data;
    }
    $model = clone $this->get_model();
    return $model->load_values($data,TRUE);
  }

  /**
   * Iterator: key
   */
  public function key()
  {
    return $this->_cursor->key();
  }

  /**
   * Iterator: next
   */
  public function next()
  {
    return $this->_cursor->next();
  }

  /**
   * Iterator: rewind
   */
  public function rewind()
  {
    if( ! $this->_cursor)
    {
      $this->load();
    }
    $this->_cursor->rewind();
  }

  /**
   * Iterator: valid
   */
  public function valid()
  {
    return $this->_cursor->valid();
  }

  /**
   * Return a string representation of the full query (in Mongo shell syntax)
   * 
   * @return  string
   */
  public function __toString()
  {
    $query = array();
    if($this->_query) $query[] = JSON::str($this->_query);
    if($this->_fields) $query[] = JSON::str(array_keys($this->_fields));
    $query = "db.$this->name.find(".implode(',',$query).")";
    foreach($this->_options as $key => $value)
    {
      $query .= ".$key(".JSON::str($value).")";
    }
    return $query;
  }

}