<?php
/* 
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

class Controller_Mongotest extends Controller {

  protected $db;

  public function before()
  {
    echo "<h1>STARTING MONGO TESTS</h1><style>span.highlight { background-color:#ffffdf }</style>";
    $this->setup();
  }

  public function after()
  {
    echo "<h1>TESTS COMPLETE</h1>";
    echo '<div id="kohana-profiler">'.View::factory('profiler/stats').'</div>';
    $this->teardown();
  }

  public function assert($desc, $condition)
  {
    if($condition)
      echo $desc.' <span class="pass">OK</span><br/>';
    else
    {
      echo $desc.' <span class="fail">FAIL</span><br/>';
      $this->teardown();
      $bt = debug_backtrace();
      $bt = array_shift($bt);
      echo "<hr/><b>{$bt['file']}: ({$bt['line']})</b><br/>";
      echo Kohana::debug_source($bt['file'], $bt['line']);
      exit;
    }
  }

  public function test($str)
  {
    echo "<hr/><b>$str</b><br/>";
  }

  public function out($str, $data = NULL)
  {
    echo "<pre>$str</pre>".($data ? Kohana::debug($data) : "");
  }

  public function setup()
  {
    $this->db = Mongo_Database::instance('mongotest', array(
      'database' => 'mongotest',
      'profiling' => TRUE
    ));

    $this->db->dropCollection('mongotest');
    $this->db->createCollection('mongotest');
  }

  public function teardown()
  {
    $this->db->dropCollection('mongotest');
  }


  public function action_document()
  {
    $this->out('Driver Version: '.Mongo::VERSION);

    $this->test('INSERT Document WITHOUT _id');
    $data = array(
      'name' => 'mongo',
      'counter' => 10,
      'set' => array('foo','bar','baz'),
      'simplenested' => array(
        'foo' => 'bar',
      ),
      'doublenested' => array(
        'foo' => array('bar' => 'baz'),
      ),
    );
    $this->out('BEFORE',$data);
    $doc = new Document();
    $doc->load_values($data);
    $doc->save();
    $this->assert('document loaded after save', $doc->loaded() === TRUE);
    $this->out('AFTER',$doc->as_array());
    $this->assert('_id exists', $doc->id);

    $this->test('RETRIEVE DOCUMENT BY _id');
    $id = $doc->id;
    $doc = new Document($id);
    $doc->load();
    $this->assert('document found', $doc->loaded() && $doc->name == 'mongo');

    $this->test('UPDATE Document');
    $doc->size = 'huge';
    $doc->save()->load();
    $this->assert('update saved', $doc->size == 'huge');

    $this->test('INCREMENT COUNTER');
    $old = $doc->counter;
    $doc->inc('counter')->save()->load();
    $this->assert('counter incremented', $old + 1 === $doc->counter);

    $this->test('UPSERT NON-EXISTING DOCUMENT');
    $doc = new Document();
    $doc->name = 'Bugs Bunny';
    $doc->push('friends','Daffy Duck');
    $doc->upsert();
    $doc->load();
    $this->assert('document inserted on upsert', !empty($doc->id));

    $this->test('UPSERT EXISTING DOCUMENT');
    $doc = new Document();
    $doc->name = 'Bugs Bunny';
    $doc->push('friends','Elmer Fudd');
    $doc->upsert();
    $doc->load();
    $this->assert('document updated on upsert', $doc->friends === array('Daffy Duck','Elmer Fudd'));

    $this->test('DELETE Document');
    $doc->delete();
    $doc->load(array('name' => 'Bugs Bunny'));
    $this->assert('document deleted', empty($doc->id));

    $this->test('INSERT Document WITH _id');
    $data = array('name' => 'mongo', 'counter' => 10, 'set' => array('foo','bar','baz'));
    $doc = new Document();
    $doc->id = 'test_doc';
    $doc->load_values($data)->save();
    $doc = new Document('test_doc');
    $doc->load();
    $this->assert('document found', $doc->loaded());
  }


  public function action_collection()
  {
    $col = new Document_Collection();

    $this->test('INSERT MULTIPLE');
    $batch = array();
    for($i = 0; $i < 20; $i++){
      $batch[] = array('name' => base64_encode(rand(0xFFF,0xFFFF)), 'number' => $i);
    }
    $col->batchInsert($batch);
    $this->assert('all records inserted', $col->count(array()) == 20);

    $this->test('ITERATE WITH FILTER LIMIT AND SORT');
    $col->reset()->find('{number: { $gt: 10 }}')->limit(6)->sort_asc('name');
    $this->assert('collection limit', count($col->as_array()) <= 6);
    $last = '';
    foreach($col as $doc){
      $this->assert("$doc->name: $doc->number ($doc->id)", $doc->number > 10 && $last < $doc->name);
      $last = $doc->name;
    }

    $col->count();
    $col->count(array('number' => array('$gt' => 10)));

    
  }

  public function action_reference()
  {
    $this->test('CREATE DOCUMENT WITH NESTED DOCUMENT');
    $doc = new Document();
    $doc->id = 'foo';
    $doc->nested = Mongo_Document::factory('document');
    $doc->nested->bar = 'baz';
    $doc->save();
    $this->assert('nested document reference created', $doc->_nested);
    $doc = new Document('foo');
    $this->assert('nested document saved',$doc->nested->bar == 'baz');
    $this->out('Data',$doc->collection()->as_array());
  }

}

class Document_Collection extends Mongo_Collection {
  public $name = 'mongotest';
  protected $db = 'mongotest';
}

class Document extends Mongo_Document {
  protected $_references = array(
    'nested' => array('model' => 'document')
  );
}

class Model_Document_Collection extends Mongo_Collection {
  public $name = 'mongotest';
  protected $db = 'mongotest';
}

class Model_Document extends Mongo_Document {
}