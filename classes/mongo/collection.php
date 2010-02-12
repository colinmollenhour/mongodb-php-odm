<?php

abstract class Mongo_Collection implements Iterator, Countable {

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

  /** @var  boolean  Whether or not this collection is a gridFS collection */
  public $gridFS;

  /** @var  string  The name of the collection within the database or the gridFS prefix if gridFS is TRUE*/
  public $name;

  /** @var  mixed  The database configuration name (passed to Mongo_Database::instance() )*/
  protected $db = 'default';

  /** @var  string  The class name of the corresponding document model (cached) */
  protected $_model; // Defaults to class name without _Collection

  /** @var  MongoCursor  The cursor instance in use while iterating a collection */
  protected $_cursor;

  /** @var  array  The current query criteria */
  protected $_query = array();
  protected $_fields = array();
  protected $_options = array();

  /**  @var  array  A cache of MongoCollection instances for performance */
  protected static $collections = array();

  /**
   * Instantiate a new collection object, can be used for querying, updating, etc..
   */
  public function __construct()
  {

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
        $bm = Profiler::start("Mongo_Database::$this->db","$this->name.$name(".implode(',',$json_arguments).")");
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
   * @return  MongoCollection
   */
  public function collection()
  {
    if( ! isset(self::$collections[$this->name]))
    {
      $selectMethod = ($this->gridFS ? 'getGridFS' : 'selectCollection');
      self::$collections[$this->name] = $this->db()->$selectMethod($this->name);
    }
    return self::$collections[$this->name];
  }

  /**
   * Set some criteria for the query. Unlike MongoCollection::find, this can be called multiple
   * times and the query paramters will be merged together.
   * 
   * Usages:
   *   $query is an array
   *   $query is a field name and $value is the value to search for
   *   $query is a JSON string that will be interpreted as the query criteria
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
    $this->_query = Arr::merge($this->_query,$query);
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
    $this->_fields = array_unique(array_merge($this->_fields,$fields));
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

    if( ! is_array($fields))
    {
      $fields = array($fields => $direction);
    }

    if( ! isset($this->_options['sort']))
    {
      $this->_options['sort'] = $fields;
    }
    else
    {
      $this->_options['sort'] = Arr::merge($this->_options['sort'], $fields);
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
    $this->_cursor = $this->collection()->find($this->_query,$this->_fields);
    foreach($this->_options as $key => $value)
    {
      if($value === NULL) $this->_cursor->$key();
      else $this->_cursor->$key($value);
    }

    if( ! $this->_model)
    {
      $this->_model = substr(get_class($this),0,-11);
    }

    if($this->db()->profiling && ! $skipBenchmark)
    {
      $this->_bm = Profiler::start("Mongo_Database::$this->db",$this->__toString());
    }

    return $this;
  }

  /**
   * Proxy method for MongoCollection::findOne, returns a corresponding Mongo_Document
   *
   * @param  array  $query
   * @param  array  $fields
   * @return  Mongo_Document
   */
  public function findOne($query = array(), array $fields = array())
  {
    if( ! $this->_model)
    {
      $this->_model = substr(get_class($this),0,-11);
    }
    $model_name = $this->_model;
    $model = new $model_name;
    return $model->load($query,$fields);
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
   * Count the results from the current query: pass FALSE for "all" results (disregard limit/skip)
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
        $bm = Profiler::start("Mongo_Database::$this->db","$this->name.count(".($query ? JSON::str($query):'').")");
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
    $model_class = $this->_model;
    $model = new $model_class;
    $data = $this->_cursor->current();

    if(isset($this->_bm))
    {
      Profiler::stop($this->_bm);
      unset($this->_bm);
    }

    $model->load_values($data,TRUE);
    return $model;
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
    if($this->_fields) $query[] = JSON::str($this->_fields);
    $query = "$this->name.find(".implode(',',$query).")";
    foreach($this->_options as $key => $value)
    {
      $query .= ".$key(".JSON::str($value).")";
    }
    return $query;
  }

}