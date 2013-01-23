<?php

require_once __DIR__ . '/../classes/json.php';
require_once __DIR__ . '/../classes/mongo/database.php';
require_once __DIR__ . '/../classes/mongo/collection.php';
require_once __DIR__ . '/../classes/mongo/document.php';

/* Row Data Gateway Pattern */

class Model_Test_Document_Collection extends Mongo_Collection {

  protected $name = 'mongotest';
  protected $db = 'mongotest';

}

class Model_Test_Document extends Mongo_Document {

  protected $_references = array(
      'other' => array('model' => 'test_other'),
      'lots'  => array('model'    => 'test_other', 'field'    => '_lots', 'multiple' => TRUE)
  );

}

/* Table Data Gateway Pattern */

class Model_Test_Other extends Mongo_Document {

  protected $name = 'mongotest';
  protected $db = 'mongotest';
  protected $_searches = array(
      'docs' => array('model' => 'test_document', 'field' => '_other'),
  );

}

/**
 */
class MongoTest extends PHPUnit_Framework_TestCase {

  /** @var Mongo_Database */
  protected $db;

  protected function setUp()
  {
    $this->db = Mongo_Database::instance('mongotest', array(
                'database' => 'mongotest',
//                    'profiling' => TRUE
            ));

    $this->db->createCollection('mongotest');
    $this->db->mongotest->remove(array());
  }

  protected function tearDown()
  {
    
  }

  public function testCollection()
  {
    $col = Mongo_Document::factory('test_document')->collection();

    $batch = array();
    for ($i = 0; $i < 20; $i++)
    {
      $batch[] = array('name'   => base64_encode(rand(0xFFF, 0xFFFF)), 'number' => $i);
    }
    $col->batchInsert($batch);
    $this->assertEquals(20, $col->count(array()), 'batch insert failed');

    $col->reset()->find('{number: { $gt: 10 }}')->limit(6)->sort_asc('name');
    $this->assertLessThanOrEqual(6, count($col->as_array()), 'limit failed');
    $last = '';
    foreach ($col as $doc)
    {
      $this->assertGreaterThan(10, $doc->number, "Failed gt $doc->name: $doc->number ($doc->id)");
      $this->assertGreaterThanOrEqual($last, $doc->name, "Failed sort $doc->name: $doc->number ($doc->id)");
      $last = $doc->name;
    }

//        $col->count();
//        $col->count(array('number' => array('$gt' => 10)));
  }

  public function testReference()
  {
    $doc = new Model_Test_Document();
    $doc->id = 'foo';
    $doc->other = Mongo_Document::factory('test_other');
    $doc->other->bar = 'baz';
    $doc->other->save();
    $doc->save();
    $this->assertNotNull($doc->_other, 'referenced document reference doesnt exist');
    $doc = new Model_Test_Document('foo');
    $this->assertEquals('baz', $doc->other->bar, 'nested document not saved');

    $docs = $doc->other->find_docs();
    $this->assertEquals(1, $docs->count(), 'doc not found');
    $doc0 = $docs->getNext();
    $this->assertEquals('foo', $doc0->id, 'doc id is expected');

    for ($i = 0; $i < 3; $i++)
    {
      $newdoc = Mongo_Document::factory('test_other')->load_values(array('id'  => 'more' . $i, 'foo' => 'bar' . $i))->save();
      $doc->push('_lots', $newdoc->id);
    }
    $doc->save();
    $lots = $doc->lots;
    $this->assertEquals(3, $lots->count(), 'should find 3 referenced docs');
  }

  public function testDocument()
  {

    $data = array(
        'name'         => 'mongo',
        'counter'      => 10,
        'set'          => array('foo', 'bar', 'baz'),
        'simplenested' => array(
            'foo' => 'bar',
        ),
        'doublenested' => array(
            'foo' => array('bar' => 'baz'),
        ),
    );
//        $this->out('BEFORE', $data);
    $doc = new Model_Test_Document();
    $doc->load_values($data);
    $doc->save();
    $this->assertTrue($doc->loaded(), 'document loaded after save');
//        $this->out('AFTER', $doc->as_array());
    $this->assertNotNull($doc->id, '_id exists');

    $id = $doc->id;
    $doc = new Model_Test_Document($id);
    $doc->load();
    $this->assertTrue($doc->loaded());
    $this->assertEquals('mongo', $doc->name);

    $doc->size = 'huge';
    $doc->save()->load();
    $this->assertEquals('huge', $doc->size, 'update not saved');

    $old = $doc->counter;
    $doc->inc('counter')->save()->load();
    $this->assertEquals($old + 1, $doc->counter, 'counter not incremented');

    $doc = new Model_Test_Document();
    $doc->name = 'Bugs Bunny';
    $doc->push('friends', 'Daffy Duck');
    $doc->upsert();
    $this->assertEmpty($doc->id, 'upsert does not get the id');
    $doc->load();
    $this->assertNotEmpty($doc->id, 'document not inserted on upsert');

    $doc = new Model_Test_Document();
    $doc->name = 'Bugs Bunny';
    $doc->push('friends', 'Elmer Fudd');
    $doc->upsert();
    $doc->load();
    $this->assertEquals(array('Daffy Duck', 'Elmer Fudd'), $doc->friends, 'document not updated on upsert');

    $doc->delete();
    $doc->load(array('name' => 'Bugs Bunny'));
    $this->assertFalse($doc->loaded(), 'document deleted');

//        $this->test('INSERT Document WITH _id');
    $data = array('name'    => 'mongo', 'counter' => 10, 'set'     => array('foo', 'bar', 'baz'));
    $doc = new Model_Test_Document();
    $doc->id = 'test_doc';
    $doc->load_values($data)->save();
    $doc = new Model_Test_Document('test_doc');
    $doc->load();
    $this->assertTrue($doc->loaded(), 'document not found');
  }

  /** @dataProvider emulationProvider  */
  public function testEmulation($data, $expected, $call)
  {
    foreach ([false, true] as $modelEmulation)
    {
      foreach ([null, false, true] as $functionEmulation)
      {
        $emulation = $functionEmulation === null ? $modelEmulation : $functionEmulation;

        unset($data['_id']);
        $doc = new Model_Test_Document();
        $doc->set_emulation($modelEmulation);
        $doc->load_values($data);
        $doc->save();

        $expected['_id'] = $doc->id;
        $data['_id'] = $doc->id;

        $args = $call;
        $method = array_shift($args);
        if ($functionEmulation !== null) $args[] = $functionEmulation;
        call_user_func_array([$doc, $method], $args);
        if ($emulation)
        {
          $this->assertEquals($expected, $doc->as_array(), "Should be emulated (model:$modelEmulation, func:$functionEmulation, effective:$emulation)");
        }
        else
        {
          $this->assertEquals($data, $doc->as_array(), "Should be untouched (model:$modelEmulation, func:$functionEmulation, effective:$emulation)");
        }
        $doc->save();
        $doc->load();
        $this->assertEquals($expected, $doc->as_array(), "After save & load (model:$modelEmulation, func:$functionEmulation, effective:$emulation)");
      }
    }
  }

  public function emulationProvider()
  {
    return [
//            [ ['num' => 1], ['num' => 1, 'foo' => 'bar'], ['set', 'foo', 'bar'] ], // without dot notation set() acts inconsistently
        'set'       => [ ['num' => 1], ['num' => 1, 'foo' => ['bar' => 'baz']], ['set', 'foo.bar', 'baz']],
        'setdot'    => [ ['num' => 1], ['num' => 1, 'foo' => ['bar']], ['set', 'foo.0', 'bar']],
        'unset'     => [ ['num' => 1], [], ['_unset', 'num']],
        'inc'       => [ ['num' => 1], ['num' => 3], ['inc', 'num', 2]],
        'push'      => [ ['foo' => ['bar']], ['foo' => ['bar', 'baz']], ['push', 'foo', 'baz']],
        'push2'     => [ ['foo' => ['bar']], ['foo' => ['bar', 'bar']], ['push', 'foo', 'bar']],
        'push3'     => [ ['a' => 'b'], ['a'   => 'b', 'foo' => ['baz']], ['push', 'foo', 'baz']],
        'pushAll'   => [ ['a' => 'b'], ['a'   => 'b', 'foo' => ['bar', 'baz']], ['pushAll', 'foo', ['bar', 'baz']]],
        'pull'      => [ ['foo' => ['bar', 'baz']], ['foo' => ['bar']], ['pull', 'foo', 'baz']],
        'pullAll'   => [ ['foo' => ['bar', 'baz']], ['foo' => []], ['pullAll', 'foo', ['bar', 'baz']]],
        'pop'       => [ ['foo' => ['bar', 'baz']], ['foo' => ['bar']], ['pop', 'foo', true]],
        'shift'     => [ ['foo' => ['bar', 'baz']], ['foo' => ['baz']], ['pop', 'foo', false]],
        'shift2'    => [ ['foo' => ['bar', 'baz']], ['foo' => ['baz']], ['shift', 'foo']],
        'addToSet'  => [ ['foo' => []], ['foo' => ['bar']], ['addToSet', 'foo', 'bar']],
        'addToSet2' => [ ['foo' => ['bar']], ['foo' => ['bar']], ['addToSet', 'foo', 'bar']],
    ];
  }

}

