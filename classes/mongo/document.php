<?php
/**
 * This class objectifies a Mongo document and can be used with one of the following design patterns:
 *
 * 1. Table Data Gateway pattern
 * <code>
 * class Model_Post extends Mongo_Document {
 *   protected $name = 'posts';
 *   // All model-related code here
 * }
 * $post = Mongo_Document::factory('post', $post_id);
 * </code>
 *
 * 2. Row Data Gateway pattern:
 * <code>
 * class Model_Post_Collection extends Mongo_Collection {
 *   protected $name = 'posts';
 *   // Collection-related code here
 * }
 * class Model_Post extends Mongo_Document {
 *   // Document-related code here
 * }
 * $post = Mongo_Document::factory('post', $post_id);
 * </code>
 *
 * The following examples could be used with either pattern with no differences in usage. The Row Data Gateway pattern is recommended
 * for more complex models to improve code organization while the Table Data Gateway pattern is recommended for simpler models.
 *
 * <code>
 *   class Model_Document extends Mongo_Document {
 *     public $name = 'test';
 *   }
 *   $document = new Model_Document(); // or Mongo_Document::factory('document');
 *   $document->name = 'Mongo';
 *   $document->type = 'db';
 *   $document->save();
 *   // db.test.save({"name":"Mongo","type":"db"});
 * </code>
 *
 * The _id is aliased to id by default. Other aliases can also be defined using the _aliases protected property. Aliases can be used
 * anywhere that a field name can be used including dot-notation for nesting.
 *
 * <code>
 *   $id = $document->id;  // MongoId
 * </code>
 *
 * All methods that take query parameters support JSON strings as input in addition to PHP arrays. The JSON parser is more lenient
 * than usual.
 *
 * <code>
 *   $document->load('{name:"Mongo"}');
 *   // db.test.findOne({"name":"Mongo"});
 * </code>
 *
 * Methods which are intended to be overridden are {before,after}_{save,load,delete} so that special actions may be
 * taken when these events occur:
 *
 * <code>
 *   public function before_save()
 *   {
 *     $this->inc('visits');
 *     $this->last_visit = time();
 *   }
 * </code>
 *
 * When a document is saved, update will be used if the document already exists, otherwise insert will be used, determined
 * by the presence of an _id. A document can be modified without being loaded from the database if an _id is passed to the constructor:
 *
 * <code>
 *   $doc = new Model_Document($id);
 * </code>
 *
 * Atomic operations and updates are not executed until save() is called and operations are chainable. Example:
 *
 * <code>
 *   $doc->inc('uses.boing');
 *       ->push('used',array('type' => 'sound', 'desc' => 'boing'));
 *   $doc->inc('uses.bonk');
 *       ->push('used',array('type' => 'sound', 'desc' => 'bonk'));
 *       ->save();
 *   // db.test.update(
 *   //   {"_id":"one"},
 *   //   {"$inc":
 *   //     {"uses.boing":1,"uses.bonk":1},
 *   //    "$pushAll":
 *   //     {"used":[{"type":"sound","desc":"boing"},{"type":"sound","desc":"bonk"}]}
 *   //   }
 *   // );
 * </code>
 *
 * Documents are loaded lazily so if a property is accessed and the document is not yet loaded, it will be loaded on the first property access:
 *
 * <code>
 *   echo "$doc->name rocks!";
 *   // Mongo rocks!
 * </code>
 *
 * Documents are reloaded when accessing a property that was modified with an operator and then saved:
 *
 * <code>
 *   in_array($doc->roles,'admin');
 *   // TRUE
 *   $doc->pull('roles','admin');
 *   in_array($doc->roles,'admin');
 *   // TRUE
 *   $doc->save();
 *   in_array($doc->roles,'admin');
 *   // FALSE
 * </code>
 *
 * Documents can have references to other documents which will be loaded lazily and saved automatically.
 *
 * <code>
 *   class Model_Post extends Mongo_Document {
 *     protected $name = 'posts';
 *     protected $_references = array('user' => array('model' => 'user'));
 *   }
 *
 *   class Model_User extends Mongo_Document {
 *     protected $name = 'users';
 *   }
 *
 *   $user = Mongo_Document::factory('user')->set('id','colin')->set('email','colin@mollenhour.com');
 *   $post = Mongo_Document::factory('post');
 *   $post->user = $user;
 *   $post->title = 'MongoDb';
 *   $post->save()
 *   // db.users.save({"_id":"colin","email":"colin@mollenhour.com"})
 *   // db.posts.save({"_id":Object,"_user":"colin","title":"MongoDb"})
 *
 *   $post = new Model_Post($id);
 *   $post->_user;
 *   // "colin" - the post was loaded lazily.
 *   $post->user->id;
 *   // "colin" - the user object was created lazily but not loaded.
 *   $post->user->email;
 *   // "colin@mollenhour.com" - now the user document was loaded as well.
 * </code>
 *
 * @author  Colin Mollenhour
 * @package Mongo_Database
 */

abstract class Mongo_Document {

  const SAVE_INSERT = 'insert';
  const SAVE_UPDATE = 'update';
  const SAVE_UPSERT = 'upsert';

  /**
   * Instantiate an object conforming to Mongo_Document conventions.
   * The document is not loaded until load() is called.
   *
   * @param   string  model name
   * @param   mixed   optional _id of document to operate on or criteria for loading (if you expect it exists)
   * @return  Mongo_Document
   */
  public static function factory($name, $load = NULL)
  {
    $class = 'Model_'.implode('_',array_map('ucfirst', explode('_',$name)));
    return new $class($load);
  }

  /** The name of the collection within the database or the gridFS prefix if gridFS is TRUE
   *
   *  If using a corresponding Mongo_Collection subclass, set this only in the Mongo_Collection subclass.
   *
   *  @var  string */
  protected $name;

  /** The database configuration name (passed to Mongo_Database::instance() )
   *
   *  If using a corresponding Mongo_Collection subclass, set this only in the Mongo_Collection subclass.
   *
   *  @var  string */
  protected $db = 'default';

  /** Whether or not this collection is a gridFS collection
   *
   *  If using a corresponding Mongo_Collection subclass, set this only in the Mongo_Collection subclass.
   *
   *  @var  boolean */
  protected $gridFS = FALSE;

  /** Definition of references existing in this document.
   * If 'model' is not specified it defaults to the reference name.
   * If 'field' is not specified it defaults to the reference name prefixed with an '_'.
   *
   * <pre>
   * Example Document:
   *  {_id:1,user_id:2,_token:3}
   *
   * protected $_references = array(
   *  'user' => array('model' => 'user', 'field' => 'user_id'),
   *  'token' => NULL,
   * );
   * </pre>
   *
   *  @var  array */
  protected $_references = array();

  /** Definition of predefined searches for use with __call. This instantiates a collection for the target model
   * and initializes the search with the specified field being equal to the _id of the current object.
   *
   * <pre>
   * $_searches
   *  {events: {model: 'event', field: '_user'}}
   * // db.event.find({_user: <_id>})
   * </pre>
   *
   * @var  array */
  protected $_searches = array();

  /** Field name aliases. '_id' is automatically aliased to 'id'.
   * E.g.: {created_at: ca}
   *  @var  array */
  protected $_aliases = array();

  /** Designated place for non-persistent data storage (will not be saved to the database or after sleep)
   *  @var  array */
  public $__data = array();

  /** Internal storage of object data
   *  @var  array */
  protected $_object = array();

  /** Keep track of fields changed using __set or load_values
   *  @var  array */
  protected $_changed = array();

  /** Set of operations to perform on update/insert
   *  @var  array */
  protected $_operations = array();

  /** Keep track of data that is dirty (changed by an operation but not yet updated from database)
   *  @var  array */
  protected $_dirty = array();

  /** Storage for referenced objects
   *  @var  array */
  protected $_related_objects = array();

  /** Document loaded status:
   * <pre>
   *   NULL   not attempted
   *   FALSE  failed
   *   TRUE   succeeded
   * </pre>
   *
   *  @var  boolean */
  protected $_loaded = NULL;

  /** A cache of Mongo_Collection instances for performance
   *  @static  array */
  protected static $collections = array();

  /**
   * Instantiate a new Document object. If an id or other data is passed then it will be assumed that the
   * document exists in the database and updates will be performaed without loading the document first.
   *
   * @param   string  The _id of the document to operate on or criteria used to load
   * @return  void
   */
  public function __construct($id = NULL)
  {
    if($id !== NULL)
    {
      if(is_array($id))
      {
        foreach($id as $key => $value)
        {
          $this->_object[$this->get_field_name($key)] = $value;
        }
      }
      else
      {
        $this->_object['_id'] = $this->_cast('_id', $id);
      }
    }
  }

  /**
   * Override to cast values when they are set with untrusted data
   *
   * @param  string  $field  The field name being set
   * @param  mixed  $value  The value being set
   */
  protected function _cast($field, $value)
  {
    switch($field)
    {
      case '_id':
        // Cast _id strings to MongoIds if they convert back and forth without changing
        if( is_string($value) && strlen($value) == 24)
        {
          $id = new MongoId($value);
          if( (string) $id == $value)
            return $id;
        }
      default:
        return $value;
    }
  }

  /**
   * This function translates an alias to a database field name.
   * Aliases are defined in $this->_aliases, and id is always aliased to _id.
   * You can override this to disable alises or define your own aliasing technique.
   *
   * @param   string  $name  The aliased field name
   * @param   boolean $dot_allowed  Use FALSE if a dot is not allowed in the field name for better performance
   * @return  string  The field name used within the database
   */
  public function get_field_name($name, $dot_allowed = TRUE)
  {
    if($name == 'id' || $name == '_id') return '_id';

    if( ! $dot_allowed || ! strpos($name,'.'))
    {
      return (isset($this->_aliases[$name])
        ? $this->_aliases[$name]
        : $name
      );
    }

    $parts = explode('.', $name, 2);
    $parts[0] = $this->get_field_name($parts[0], FALSE);
    return implode('.', $parts);
  }

  /**
   * Returns the attributes that should be serialized.
   *
   * @return  void
   */
  public function __sleep()
  {
    return array('_references', '_aliases', '_object', '_changed', '_operations', '_loaded', '_dirty');
  }

  /**
   * Checks if a field is set
   *
   * @return  boolean  field is set
   */
  public function __isset($name)
  {
    $name = $this->get_field_name($name, FALSE);
    return isset($this->_object[$name]);
  }

  /**
   * Unset a field
   *
   * @return  void
   */
  public function __unset($name)
  {
    $this->_unset($name);
  }

  /**
   * Clear the document data
   *
   * @return  Mongo_Document
   */
  public function clear()
  {
    $this->_object = $this->_changed = $this->_operations = $this->_dirty = $this->_related_objects = array();
    $this->_loaded = NULL;
    return $this;
  }

  /**
   * Return TRUE if field has been changed
   *
   * @param   string   $name  field name (no parameter returns TRUE if there are *any* changes)
   * @return  boolean  field has been changed
   */
  public function is_changed($name = NULL)
  {
    if($name === NULL)
    {
      return ($this->_changed || $this->_operations);
    }
    else
    {
      $name = $this->get_field_name($name);
      return isset($this->_changed[$name]) || isset($this->_dirty[$name]);
    }
  }

  /**
   * Return the Mongo_Database reference (proxy to the collection's db() method)
   *
   * @return  Mongo_Database
   */
  public function db()
  {
    return $this->collection()->db();
  }

  /**
   * Get a corresponding collection singleton
   *
   * @param  boolean  $fresh  Pass TRUE if you don't want to get the singleton instance
   * @return Mongo_Collection
   */
  public function collection($fresh = FALSE)
  {
    if($fresh === TRUE)
    {
      if($this->name)
      {
        return new Mongo_Collection($this->name, $this->db, $this->gridFS, get_class($this));
      }
      else
      {
        $class_name = $this->get_collection_class_name();
        return new $class_name(NULL, NULL, NULL, get_class($this));
      }
    }

    if($this->name)
    {
      $name = "$this->db.$this->name.$this->gridFS";
      if( ! isset(self::$collections[$name]))
      {
        self::$collections[$name] = new Mongo_Collection($this->name, $this->db, $this->gridFS, get_class($this));
      }
      return self::$collections[$name];
    }
    else
    {
      $name = $this->get_collection_class_name();
      if( ! isset(self::$collections[$name]))
      {
        self::$collections[$name] = new $name(NULL, NULL, NULL, get_class($this));
      }
      return self::$collections[$name];
    }
  }

  /**
   * Generates the collection name
   * @return  string
   */
  protected function get_collection_class_name()
  {
    return get_class($this).'_Collection';
  }

  /**
   * Current magic methods supported:
   *
   *  find_<search>()  -  Perform predefined search (using key from $_searches)
   *
   * @param  string $name
   * @param  array  $arguments
   * @return Mongo_Collection
   */
  public function __call($name, $arguments)
  {
    // Workaround Reserved Keyword 'unset'
    // http://php.net/manual/en/reserved.keywords.php
    if($name == 'unset')
    {
        return $this->_unset($arguments[0]);
    }

    $parts = explode('_', $name, 2);
    if( ! isset($parts[1]))
    {
      trigger_error('Method not found by '.get_class($this).': '.$name);
    }

    switch($parts[0])
    {
      case 'find':
        $search = $parts[1];
        if( ! isset($this->_searches[$search])){
          trigger_error('Predefined search not found by '.get_class($this).': '.$search);
        }
        return Mongo_Document::factory($this->_searches[$search]['model'])
                ->collection(TRUE)
                ->find(array($this->_searches[$search]['field'] => $this->_id));
      break;

      default:
        trigger_error('Method not found by '.get_class($this).': '.$name);
      break;
    }
  }

  /**
   * Gets one of the following:
   *
   *  - A referenced object
   *  - A search() result
   *  - A field's value
   *
   * @param   string  field name
   * @return  mixed
   */
  public function __get($name)
  {
    $name = $this->get_field_name($name, FALSE);

    // Auto-loading for special references
    if(array_key_exists($name, $this->_references))
    {
      if( ! isset($this->_related_objects[$name]))
      {
        $model = isset($this->_references[$name]['model']) ? $this->_references[$name]['model'] : $name;
        $foreign_field = isset($this->_references[$name]['foreign_field']) ? $this->_references[$name]['foreign_field'] : FALSE;
        if ($foreign_field) {
          $this->_related_objects[$name] = Mongo_Document::factory($model)
            ->collection(TRUE)
            ->find($foreign_field, $this->id);
          return $this->_related_objects[$name];
        }
        $id_field = isset($this->_references[$name]['field']) ? $this->_references[$name]['field'] : "_$name";
        $value = $this->__get($id_field);

        if( ! empty($this->_references[$name]['multiple']))
        {
          $this->_related_objects[$name] = Mongo_Document::factory($model)
                  ->collection(TRUE)
                  ->find(array('_id' => array('$in' => (array) $value)));
        }
        else
        {
          // Extract just id if value is a DBRef
          if(is_array($value) && isset($value['$id']))
          {
            $value = $value['$id'];
          }
          $this->_related_objects[$name] = Mongo_Document::factory($model, $value);
        }
      }
      return $this->_related_objects[$name];
    }

    // Reload when retrieving dirty data
    if($this->_loaded && empty($this->_operations) && ! empty($this->_dirty[$name]))
    {
      $this->load();
    }

    // Lazy loading!
    else if($this->_loaded === NULL && isset($this->_object['_id']) && ! isset($this->_changed['_id']) && $name != '_id')
    {
      $this->load();
    }

    return isset($this->_object[$name]) ? $this->_object[$name] : NULL;
  }

  /**
   * Magic method for setting the value of a field. In order to set the value of a nested field,
   * you must use the "set" method, not the magic method. Examples:
   *
   * <code>
   * // Works
   * $doc->set('address.city', 'Knoxville');
   *
   * // Does not work
   * $doc->address['city'] = 'Knoxville';
   * </code>
   *
   * @param   string  field name
   * @param   mixed   new field value
   * @return  mixed
   */
  public function __set($name, $value)
  {
    $name = $this->get_field_name($name, FALSE);

    // Automatically save references to other Mongo_Document objects
    if(array_key_exists($name, $this->_references))
    {
      if( ! $value instanceof Mongo_Document)
      {
        throw new Exception('Cannot set reference to object that is not a Mongo_Document');
      }
      $this->_related_objects[$name] = $value;
      if(isset($value->_id))
      {
        $id_field = isset($this->_references[$name]['field']) ? $this->_references[$name]['field'] : "_$name";
        $this->__set($id_field, $value->_id);
      }
      return;
    }

    // Do not save sets that result in no change
    $value = $this->_cast($name, $value);
    if ( isset($this->_object[$name]) && $this->_object[$name] === $value)
    {
      return;
    }

    $this->_object[$name] = $value;
    $this->_changed[$name] = TRUE;
  }

  protected function _set_dirty($name)
  {
    if($pos = strpos($name,'.'))
    {
      $name = substr($name,0,$pos);
    }
    $this->_dirty[$name] = TRUE;
    return $this;
  }

  /**
   * Set the value for a key. This function must be used when updating nested documents.
   *
   * @param   string  $name The key of the data to update (use dot notation for embedded objects)
   * @param   mixed   $value The data to be saved
   * @return  Mongo_Document
   */
  public function set($name, $value)
  {
    if( ! strpos($name, '.')) {
      $this->__set($name, $value);
      return $this;
    }
    $name = $this->get_field_name($name);
    $this->_operations['$set'][$name] = $value;
    return $this->_set_dirty($name);
  }

  /**
   * Unset a key
   *
   * Note: unset() method call for _unset() is defined in __call() method since 'unset' method name
   *       is reserved in PHP. ( Requires PHP > 5.2.3. - http://php.net/manual/en/reserved.keywords.php )
   *
   * @param   string  $name The key of the data to update (use dot notation for embedded objects)
   * @return Mongo_Document
   */
  public function _unset($name)
  {
    $name = $this->get_field_name($name);
    $this->_operations['$unset'][$name] = 1;
    return $this->_set_dirty($name);
  }

  /**
   * Increment a value atomically
   *
   * @param   string  $name The key of the data to update (use dot notation for embedded objects)
   * @param   mixed   $value The amount to increment by (default is 1)
   * @return  Mongo_Document
   */
  public function inc($name, $value = 1)
  {
    $name = $this->get_field_name($name);
    if(isset($this->_operations['$inc'][$name]))
    {
      $this->_operations['$inc'][$name] += $value;
    }
    else
    {
      $this->_operations['$inc'][$name] = $value;
    }
    return $this->_set_dirty($name);
  }

  /**
   * Push a vlaue to an array atomically. Can be called multiple times.
   *
   * @param   string  $name The key of the data to update (use dot notation for embedded objects)
   * @param   mixed   $value The value to push
   * @return  Mongo_Document
   */
  public function push($name, $value)
  {
    $name = $this->get_field_name($name);
    if(isset($this->_operations['$pushAll'][$name]))
    {
      $this->_operations['$pushAll'][$name][] = $value;
    }
    else if(isset($this->_operations['$push'][$name]))
    {
      $this->_operations['$pushAll'][$name] = array($this->_operations['$push'][$name],$value);
      unset($this->_operations['$push'][$name]);
      if( ! count($this->_operations['$push']))
        unset($this->_operations['$push']);
    }
    else
    {
      $this->_operations['$push'][$name] = $value;
    }
    return $this->_set_dirty($name);
  }

  /**
   * Push an array of values to an array in the document
   *
   * @param   string  $name The key of the data to update (use dot notation for embedded objects)
   * @param   array   $value An array of values to push
   * @return  Mongo_Document
   */
  public function pushAll($name, $value)
  {
    $name = $this->get_field_name($name);
    if(isset($this->_operations['$pushAll'][$name]))
    {
      $this->_operations['$pushAll'][$name] += $value;
    }
    else
    {
      $this->_operations['$pushAll'][$name] = $value;
    }
    return $this->_set_dirty($name);
  }

  /**
   * Pop a value from the end of an array
   *
   * @param   string  $name The key of the data to update (use dot notation for embedded objects)
   * @return  Mongo_Document
   */
  public function pop($name)
  {
    $name = $this->get_field_name($name);
    $this->_operations['$pop'][$name] = 1;
    return $this->_set_dirty($name);
  }

  /**
   * Pop a value from the beginning of an array
   *
   * @param   string  $name The key of the data to update (use dot notation for embedded objects)
   * @return  Mongo_Document
   */
  public function shift($name)
  {
    $name = $this->get_field_name($name);
    $this->_operations['$pop'][$name] = -1;
    return $this->_set_dirty($name);
  }

  /**
   * Pull (delete) a value from an array
   *
   * @param   string  $name The key of the data to update (use dot notation for embedded objects)
   * @param   mixed   $value
   * @return  Mongo_Document
   */
  public function pull($name, $value)
  {
    $name = $this->get_field_name($name);
    if(isset($this->_operations['$pullAll'][$name]))
    {
      $this->_operations['$pullAll'][$name][] = $value;
    }
    else if(isset($this->_operations['$pull'][$name]))
    {
      $this->_operations['$pullAll'][$name] = array($this->_operations['$pull'][$name],$value);
      unset($this->_operations['$pull'][$name]);
      if( ! count($this->_operations['$pull']))
        unset($this->_operations['$pull']);
    }
    else
    {
      $this->_operations['$pull'][$name] = $value;
    }
    return $this->_set_dirty($name);
  }

  /**
   * Pull (delete) all of the given values from an array
   *
   * @param   string  $name The key of the data to update (use dot notation for embedded objects)
   * @param   array   $value An array of value to pull from the array
   * @return  Mongo_Document
   */
  public function pullAll($name, $value)
  {
    $name = $this->get_field_name($name);
    if(isset($this->_operations['$pullAll'][$name]))
    {
      $this->_operations['$pullAll'][$name] += $value;
    }
    else
    {
      $this->_operations['$pullAll'][$name] = $value;
    }
    return $this->_set_dirty($name);
  }

  /**
   * Bit operators
   *
   * @param   string  $name The key of the data to update (use dot notation for embedded objects)
   * @return  Mongo_Document
   */
  public function bit($name,$value)
  {
    $name = $this->get_field_name($name);
    $this->_operations['$bit'][$name] = $value;
    return $this->_set_dirty($name);
  }

  /**
   * Adds value to the array only if its not in the array already.
   *
   * @param   string  $name The key of the data to update (use dot notation for embedded objects)
   * @param   mixed   $value  The value to add to the set
   * @return  Mongo_Document
   */
  public function addToSet($name, $value)
  {
    $name = $this->get_field_name($name);
    if(isset($this->_operations['$addToSet'][$name]))
    {
      if( ! isset($this->_operations['$addToSet'][$name]['$each']))
      {
        $this->_operations['$addToSet'][$name] = array('$each' => array($this->_operations['$addToSet'][$name]));
      }
      if(isset($value['$each']))
      {
        foreach($value['$each'] as $val)
        {
          $this->_operations['$addToSet'][$name]['$each'][] = $val;
        }
      }
      else
      {
        $this->_operations['$addToSet'][$name]['$each'][] = $value;
      }
    }
    else
    {
      $this->_operations['$addToSet'][$name] = $value;
    }
    return $this->_set_dirty($name);
  }

  /**
   * Load all of the values in an associative array. Ignores all fields
   * not in the model.
   *
   * @param   array    field => value pairs
   * @param   boolean  values are clean (from database)?
   * @return  Mongo_Document
   */
  public function load_values($values, $clean = FALSE)
  {
    if($clean === TRUE)
    {
      $this->before_load($values);

      $this->_object = (array) $values;
      $this->_loaded = ! empty($this->_object);

      $this->after_load();
    }
    else
    {
      foreach ($values as $field => $value)
      {
        $this->__set($field, $value);
      }
    }

    return $this;
  }

  /**
   * Get the model data as an associative array.
   *
   * @param   boolean  retrieve values directly from _object
   * @return  array  field => value
   */
  public function as_array( $clean = FALSE )
  {
    if($clean === TRUE)
    {
      $array = $this->_object;
    }
    else
    {
      $array = array();
      foreach($this->_object as $name => $value)
      {
        $array[$name] = isset($this->_object[$name]) ? $this->_object[$name] : NULL;
      }
      foreach($this->_aliases as $alias => $name)
      {
        if(isset($array[$name]))
        {
          $array[$alias] = $array[$name];
          unset($array[$name]);
        }
      }
    }

    return $array;
  }

  /**
   * Return true if the document is loaded.
   *
   * @return  boolean
   */
  public function loaded()
  {
    if($this->_loaded === NULL)
    {
      $this->load();
    }
    return $this->_loaded;
  }

  /**
   * Load the document from the database. The first parameter may be one of:
   *
   *  a falsey value - the object data will be used to construct the query
   *  a JSON string - will be parsed and used for the query
   *  an non-array value - the query will be assumed to be for an _id of this value
   *  an array - the array will be used for the query
   *
   * @param   array  specify additional criteria
   * @param   array  specify the fields to return
   * @return  boolean  TRUE if the load succeeded
   */
  public function load($criteria = array(), array $fields = array())
  {
    // Use of json for querying is allowed
    if(is_string($criteria) && $criteria[0] == "{")
    {
      $criteria = JSON::arr($criteria);
    }

    else if($criteria && ! is_array($criteria))
    {
      $criteria = array('_id' => $criteria);
    }

    else if(isset($this->_object['_id']))
    {
      $criteria = array('_id' => $this->_object['_id']);
    }

    else if(isset($criteria['id']))
    {
      $criteria = array('_id' => $criteria['id']);
    }

    else if( ! $criteria)
    {
      $criteria = $this->_object;
    }

    if( ! $criteria)
    {
       throw new MongoException('Cannot find '.get_class($this).' without _id or other search criteria.');
    }

    // Cast query values to the appropriate types and translate aliases
    $new = array();
    foreach($criteria as $key => $value)
    {
      $key = $this->get_field_name($key);
      $new[$key] = $this->_cast($key, $value);
    }
    $criteria = $new;

    // Translate field aliases
    $fields = array_map(array($this,'get_field_name'), $fields);

    $values = $this->collection()->__call('findOne', array($criteria, $fields));

    // Only clear the object if necessary
    if($this->_loaded !== NULL || $this->_changed || $this->_operations)
    {
      $this->clear();
    }

    $this->load_values($values, TRUE);
    return $this->_loaded;
  }

  /**
   * Save the document to the database. For newly created documents the _id will be retrieved.
   *
   * @param   boolean  $safe  If FALSE the insert status will not be checked
   * @return  Mongo_Document
   */
  public function save($safe = TRUE)
  {
    // Update references to referenced models
    $this->_update_references($safe);

    // Insert new record if no _id or _id was set by user
    if( ! isset($this->_object['_id']) || isset($this->_changed['_id']))
    {
      $action = self::SAVE_INSERT;

      $this->before_save($action);

      $values = array();
      foreach($this->_changed as $name => $_true)
      {
        $values[$name] = $this->_object[$name];
      }

      if(empty($values))
      {
        throw new MongoException('Cannot insert empty array.');
      }

      $err = $this->collection()->insert($values, $safe);

      if( $safe && $err['err'] )
      {
        throw new MongoException('Unable to insert '.get_class($this).': '.$err['err']);
      }

      if ( ! isset($this->_object['_id']))
      {
        // Store (assigned) MongoID in object
        $this->_object['_id'] = $values['_id'];
        $this->_loaded = TRUE;
      }

      // Save any additional operations
      /** @todo  Combine operations into the insert when possible to avoid this update */
      if($this->_operations)
      {
        if( ! $this->collection()->update(array('_id' => $this->_object['_id']), $this->_operations))
        {
          $err = $this->db()->last_error();
          throw new MongoException('Update of '.get_class($this).' failed: '.$err['err']);
        }
      }
    }

    // Update assumed existing document
    else
    {
      $action = self::SAVE_UPDATE;

      $this->before_save($action);

      if($this->_changed)
      {
        foreach($this->_changed as $name => $_true)
        {
          $this->_operations['$set'][$name] = $this->_object[$name];
        }
      }

      if($this->_operations)
      {
        if( ! $this->collection()->update(array('_id' => $this->_object['_id']), $this->_operations))
        {
          $err = $this->db()->last_error();
          throw new MongoException('Update of '.get_class($this).' failed: '.$err['err']);
        }
      }
    }

    $this->_changed = $this->_operations = array();

    $this->after_save($action);

    return $this;
  }

  protected function _update_references($safe = TRUE)
  {
    foreach($this->_references as $name => $ref)
    {
      if(isset($this->_related_objects[$name]) && $this->_related_objects[$name] instanceof Mongo_Document)
      {
        $model = $this->_related_objects[$name];
        $id_field = isset($ref['field']) ? $ref['field'] : "_$name";
        if( ! $this->__isset($id_field) || $this->__get($id_field) != $model->_id)
        {
          $this->__set($id_field, $model->_id);
        }
      }
    }
  }

  /**
   * Override this method to take certain actions before the data is saved
   *
   * @param   string  $action  The type of save action, one of Mongo_Document::SAVE_*
   */
  protected function before_save($action){}

  /**
   * Override this method to take actions after data is saved
   *
   * @param   string  $action  The type of save action, one of Mongo_Document::SAVE_*
   */
  protected function after_save($action){}

  /**
   * Override this method to take actions before the values are loaded
   * @param    array   $values
   */
  protected function before_load($values){}

  /**
   * Override this method to take actions after the values are loaded
   */
  protected function after_load(){}

  /**
   * Override this method to take actions before the document is deleted
   */
  protected function before_delete(){}

  /**
   * Override this method to take actions after the document is deleted
   */
  protected function after_delete(){}

  /**
   * Upsert the document, does not retrieve the _id of the upserted document.
   *
   * @param   array $operations
   * @return  Mongo_Document
   */
  public function upsert($operations = array())
  {
    if( ! $this->_object)
    {
      throw new MongoException('Cannot upsert '.get_class($this).': no criteria');
    }

    $this->before_save(self::SAVE_UPSERT);

    $operations = self::array_merge_recursive_distinct($this->_operations, $operations);

    if( ! $this->collection()->update($this->_object, $operations, array('upsert' => TRUE)))
    {
      $err = $this->db()->last_error();
      throw new MongoException('Upsert of '.get_class($this).' failed: '.$err['err']);
    }

    $this->_changed = $this->_operations = array();

    $this->after_save();

    return $this;
  }

  /**
   * Delete the current document using the current data. The document does not have to be loaded.
   * Use $doc->collection()->remove($criteria) to delete multiple documents.
   *
   * @return  Mongo_Document
   */
  public function delete()
  {
    if( ! isset($this->_object['_id']))
    {
      throw new MongoException('Cannot delete '.get_class($this).' without the _id.');
    }
    $this->before_delete();
    $criteria = array('_id' => $this->_object['_id']);

    if( ! $this->collection()->remove($criteria, TRUE))
    {
      throw new MongoException('Failed to delete '.get_class($this));
    }

    $this->clear();
    $this->after_delete();

    return $this;
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

