<?php
/**
 * This class can be used in any of the following ways:
 *
 * 1. Directly as a wrapper for MongoCollection/MongoCursor:
 * <code>
 * $posts = new Mongo_Collection('posts');
 * $posts->sort_desc('published')->limit(10)->as_array(); // array of arrays
 * </code>
 *
 * 2. As part of the Table Data Gateway pattern
 * <code>
 * class Model_Post extends Mongo_Document {
 *   protected $name = 'posts';
 *   // All model-related code here
 * }
 * $posts = Mongo_Document::factory('post')->collection(TRUE);
 * $posts->sort_desc('published')->limit(10)->as_array(); // array of Model_Post
 * </code>
 *
 * 3. As part of the Row Data Gateway pattern:
 * <code>
 * class Model_Post_Collection extends Mongo_Collection {
 *   protected $name = 'posts';
 *   // Collection-related code here
 * }
 * class Model_Post extends Mongo_Document {
 *   // Document-related code here
 * }
 * $posts = Mongo_Document::factory('post')->collection(TRUE);
 * $posts->sort_desc('published')->limit(10)->as_array(); // array of Model_Post
 * </code>
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
   * @deprecated
   */
  public static function factory($name)
  {
    return Mongo_Document::factory($name)->collection(TRUE);
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

  /** The class name or instance of the corresponding document model or NULL if direct mode
   *  @var  mixed */
  protected $_model;

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

  /** A cache of Mongo_Document model instances for performance
   *  @static  array */
  protected static $models = array();

  /**
   * Instantiate a new collection object, can be used for querying, updating, etc..
   *
   * @param  string  $name  The collection name
   * @param  string  $db    The database configuration name
   * @param  boolean $gridFS  Is the collection a gridFS instance?
   */
  public function __construct($name = NULL, $db = 'default', $gridFS = FALSE, $model = FALSE)
  {
    if($name !== NULL)
    {
      $this->db = $db;
      $this->name = $name;
      $this->gridFS = $gridFS;
    }
    if($model)
    {
      $this->_model = $model;
    }
  }

  /**
   * Reset the state of the query (must be called manually if re-using a collection for a new query)
   *
   * @return  Mongo_Collection
   */
  public function reset($cursor_only = FALSE)
  {
    if( ! $cursor_only) {
      $this->_query = $this->_fields = $this->_options = array();
    }
    $this->_cursor = NULL;
    return $this;
  }

  /**
   * Magic method override. Passes on method calls to either the MongoCursor or the MongoCollection
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
        $json_arguments = array(); foreach($arguments as $arg) $json_arguments[] = json_encode((is_array($arg) ? (object)$arg : $arg));
        $bm = $this->db()->profiler_start("Mongo_Database::$this->db","db.$this->name.$name(".implode(',',$json_arguments).")");
      }

      $return = call_user_func_array(array($this->collection(), $name), $arguments);

      if(isset($bm))
      {
        $this->db()->profiler_stop($bm);
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
    $name = "$this->db.$this->name.$this->gridFS";
    if( ! isset(self::$collections[$name]))
    {
      $selectMethod = ($this->gridFS ? 'getGridFS' : 'selectCollection');
      self::$collections[$name] = $this->db()->db()->$selectMethod($this->name);
    }
    return self::$collections[$name];
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
    if( ! is_array($query))
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
      // Special purpose condition
      if($field[0] == '$')
      {
        // $or and $where and possibly other special values
        if($field == '$or')
        {
          if( ! isset($this->_query['$or']))
          {
            $this->_query['$or'] = array();
          }
          $this->_query['$or'][] = $value;
        }
        else if($field == '$where')
        {
          $this->_query['$where'] = $value;
        }
        else
        {
          $query_fields[$field] = $value;
        }
      }

      // Simple key = value condition
      else
      {
        $query_fields[$this->get_field_name($field)] = $value;
      }
    }

    $this->_query = self::array_merge_recursive_distinct($this->_query, $query_fields);
    return $this;
  }

  /**
   * Add fields to be returned by the query.
   *
   * @param   array $fields
   * @return  Mongo_collection
   */
  public function fields($fields = array(), $include = 1)
  {
    if($this->_cursor) throw new MongoCursorException('The cursor has already started iterating.');

    // Map array to hash
    if($fields == array_values($fields))
    {
      $fields = array_fill_keys($fields, $include);
    }

    // Translate field aliases
    foreach($fields as $field => $value)
    {
      $this->_fields[$this->get_field_name($field)] = $value;
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
    return $this->set_option('hint', $key_pattern);
  }

  /**
   * Sets whether this cursor will timeout
   *
   * @param   boolean
   * @return  Mongo_Collection
   */
  public function immortal($liveForever = TRUE)
  {
    return $this->set_option('immortal', $liveForever);
  }

  /**
   * Limits the number of results returned
   *
   * @param   int
   * @return  Mongo_Collection
   */
  public function limit($num)
  {
    return $this->set_option('limit', $num);
  }

  /**
   * Skips a number of results
   *
   * @param   int
   * @return  Mongo_Collection
   */
  public function skip($num)
  {
    return $this->set_option('skip', $num);
  }

  /**
   * Sets whether this query can be done on a slave
   *
   * @param   boolean
   * @return  Mongo_Collection
   */
  public function slaveOkay($okay = TRUE)
  {
    return $this->set_option('slaveOkay', $okay);
  }

  /**
   * Use snapshot mode for the query
   *
   * @return  Mongo_Collection
   */
  public function snapshot()
  {
    return $this->set_option('snapshot', NULL);
  }

  /**
   * Sorts the results by given fields
   *
   * @param   mixed  A sort criteria or a key (requires corresponding $value)
   * @param   mixed  The direction if $fields is a key
   * @return  Mongo_Collection
   */
  public function sort($fields, $direction = self::ASC)
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
      if(is_string($direction))
      {
        if($direction == 'asc' || $direction == '1')
          $direction = self::ASC;
        else
          $direction = self::DESC;
      }

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
    return $this->set_option('tailable', $tail);
  }

  /**
   * See if a cursor has an option to be set before executing the query.
   *
   * @param  string  $name
   * @return boolean
   */
  public function has_option($name)
  {
    return array_key_exists($name, $this->_options);
  }

  /**
   * Get a cursor option to be set before executing the query.
   *
   * @param  string  $name
   * @return mixed
   */
  public function get_option($name)
  {
    return isset($this->_options[$name]) ? $this->_options[$name] : NULL;
  }

  /**
   * Set a cursor option to be set before executing the query.
   *
   * @param  string  $name
   * @param  mixed  $value
   * @return Mongo_Collection
   */
  public function set_option($name, $value)
  {
    if($this->_cursor) throw new MongoCursorException('The cursor has already started iterating.');
    $this->_options[$name] = $value;
    return $this;
  }

  /**
   * Unset a cursor option to be set before executing the query.
   *
   * @param  string  $name
   * @return Mongo_Collection
   */
  public function unset_option($name)
  {
    if($this->_cursor) throw new MongoCursorException('The cursor has already started iterating.');
    unset($this->_options[$name]);
    return $this;
  }

  /**
   * Is the query executed yet?
   * 
   * @return boolean
   */
  public function is_loaded()
  {
    return !!$this->_cursor;
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
    $this->_cursor = $this->collection()->find($this->_query, $this->_fields);
    foreach($this->_options as $key => $value)
    {
      if($value === NULL) $this->_cursor->$key();
      else $this->_cursor->$key($value);
    }

    if($this->db()->profiling && ! $skipBenchmark)
    {
      $this->_bm = $this->db()->profiler_start("Mongo_Database::$this->db",$this->inspect());
    }

    return $this;
  }

  /**
   * Wrapper for MongoCollection#findOne which adds field name translations and allows query to be a single _id
   *
   * @param  mixed  $query  An _id, a JSON encoded query or an array by which to search
   * @param  array  $fields Fields of the results to return
   * @return mixed  Record matching query or NULL
   */
  public function findOne($query = array(), $fields = array())
  {
    // String query is either JSON encoded or an _id
    if( ! is_array($query))
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
        $query = array('_id' => $value);
      }
    }

    // Translate field aliases
    $query_trans = array();
    foreach($query as $field => $value)
    {
      $query_trans[$this->get_field_name($field)] = $value;
    }

    $fields_trans = array();
    if($fields && is_int(key($fields)))
    {
      $fields = array_fill_keys($fields, 1);
    }
    foreach($fields as $field => $value)
    {
      $fields_trans[$this->get_field_name($field)] = $value;
    }

    return $this->__call('findOne', array($query_trans, $fields_trans));
  }

  /**
   * Get an instance of the corresponding document model.
   *
   * @return  Mongo_Document
   */
  protected function get_model()
  {
    if( ! isset(self::$models[$this->_model]))
    {
      $model = $this->_model;
      self::$models[$this->_model] = new $model;
    }
    return self::$models[$this->_model];
  }

  /**
   * Translate a field name according to aliases defined in the model if they exist.
   *
   * @param  string $name
   * @return string
   */
  public function get_field_name($name)
  {
    if( ! $this->_model)
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
    $this->_cursor OR $this->load();
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
      foreach($this->cursor() as $key => $value)
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

    foreach($this->cursor() as $data)
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

  /**
   * Emulate an SQL "NATURAL JOIN" when there is a 1-1 or n-1 relationship with one additional query
   * for all related documents
   *
   * @param string $model_field
   * @param string $id_field
   * @return array
   */
  public function natural_join($model_field, $id_field = NULL)
  {
    if( ! $id_field) {
      $id_field = "_$model_field";
    }

    $left = $this->as_array();
    $right_ids = array();
    foreach($left as $doc)
    {
      $right_id = $doc->$id_field;
      if($right_id)
      {
        $right_ids[$right_id] = TRUE;
      }
    }
    if($right_ids)
    {
      $right = $this->get_model()->$model_field->collection(TRUE)
                  ->find(array(
                    '_id' => array('$in' => array_keys($right_ids)))
                  )
                  ->as_array();
      foreach($left as $doc)
      {
        if(isset($right[$doc->$id_field]))
        {
          $doc->$model_field = $right[$doc->$id_field];
        }
      }
    }
    return $left;
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
        $bm = $this->db()->profiler_start("Mongo_Database::$this->db",$this->inspect().".count(".JSON::str($query).")");
      }

      $this->_cursor OR $this->load(TRUE);

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
      $query_trans = array();
      foreach($query as $field => $value)
      {
        $query_trans[$this->get_field_name($field)] = $value;
      }
      $query = $query_trans;

      // Profile count operation for collection
      if($this->db()->profiling)
      {
        $bm = $this->db()->profiler_start("Mongo_Database::$this->db","db.$this->name.count(".($query ? JSON::str($query):'').")");
      }

      $count = $this->collection()->count($query);
    }

    // End profiling count
    if(isset($bm))
    {
      $this->db()->profiler_stop($bm);
    }

    return $count;
  }

  /**
   * Implement MongoCursor#hasNext to ensure that the cursor is loaded
   *
   * @return  boolean
   */
  public function hasNext()
  {
    return $this->cursor()->hasNext();
  }

  /**
   * Implement MongoCursor#getNext so that the return value is a Mongo_Document instead of array
   *
   * @return  Mongo_Document
   */
  public function getNext()
  {
    $this->cursor()->next();
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
      $this->db()->profiler_stop($this->_bm);
      unset($this->_bm);
    }

    if( ! $this->_model)
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
    $this->cursor()->rewind();
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
  public function inspect()
  {
    $query = array();
    if($this->_query) $query[] = JSON::str($this->_query);
    if($this->_fields) $query[] = JSON::str($this->_fields);
    $query = "db.$this->name.find(".implode(',',$query).")";
    foreach($this->_options as $key => $value)
    {
      $query .= ".$key(".JSON::str($value).")";
    }
    return $query;
  }

  /**
   * Return the collection name
   *
   * @return string
   */
  public function  __toString()
  {
    return $this->name;
  }

  /**
   * array_merge_recursive_distinct does not change the datatypes of the values in the arrays.
   * @param array $array1
   * @param array $array2
   * @return array
   * @author Daniel <daniel (at) danielsmedegaardbuus (dot) dk>
   * @author Gabriel Sobrinho <gabriel (dot) sobrinho (at) gmail (dot) com>
   */
  protected static function array_merge_recursive_distinct ( array &$array1, array &$array2 )
  {
    $merged = $array1;

    foreach ( $array2 as $key => &$value )
    {
      if ( is_array ( $value ) && isset ( $merged [$key] ) && is_array ( $merged [$key] ) )
      {
        $merged [$key] = self::array_merge_recursive_distinct ( $merged [$key], $value );
      }
      else
      {
        $merged [$key] = $value;
      }
    }

    return $merged;
  }

}
