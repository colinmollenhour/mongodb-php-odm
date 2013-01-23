<?php

require_once __DIR__ . '/MongoTest.php';
require_once __DIR__ . '/../classes/mongo/subdocument.php';

class Mongo_Test_Subdocument extends Mongo_Subdocument {

  public function increment()
  {
    $this->inc(false, 1, true);
  }

}

/**
 */
class MongoSubdocumentTest extends PHPUnit_Framework_TestCase {

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

  public function testCreate()
  {
    $doc = new Model_Test_Document();
    $doc->some = 'thing';
    $sub = new Mongo_Subdocument($doc, 'sub');
    $sub->foo = 'bar';
    $sub->push('bar', 'baz', true);
    $doc->save();
    $doc->load();
    $this->assertEquals(array(
        'some' => 'thing',
        'sub'  => array('foo' => 'bar', 'bar' => array('baz')),
        '_id'  => $doc->id)
            , $doc->as_array());
  }

  public function testCreateArray()
  {
    $doc = new Model_Test_Document();
    $doc->some = 'thing';
    $doc->sub = array();
    $sub = new Mongo_Subdocument($doc, 'sub.0');
    $sub->foo = 'bar';
    $sub = new Mongo_Subdocument($doc, 'sub.1');
    $sub->foo = 'baz';
    $doc->save();

    $this->assertEquals(array(
        'some' => 'thing',
        'sub'  => array(array('foo' => 'bar'), array('foo' => 'baz')),
        '_id'  => $doc->id)
            , $doc->as_array());

    $this->assertEquals(1, $doc->collection(true)->find(array('sub.0.foo' => 'bar'))->count(), 'Not created properly');
    $this->assertEquals(1, $doc->collection(true)->find(array('sub.foo' => 'bar'))->count(), 'Object, not an array!');
  }

  public function testIterate()
  {
    $doc = new Model_Test_Document();
    $doc->array = [0, 1, 2, 3, 4, 5];
    $doc->save();

    $doc->load();
    foreach (Mongo_Test_Subdocument::iterate($doc, 'array') as $i => $sub)
    {
      $this->assertInstanceOf('Mongo_Test_Subdocument', $sub);
      $this->assertEquals($i, $sub->get());
      $sub->increment();
      $this->assertEquals($i + 1, $sub->get());
    }
    $doc->save();

    $doc->load();

    $this->assertEquals([1, 2, 3, 4, 5, 6], $doc->array);
  }

  /** @dataProvider emulationProvider  */
  public function testEmulation($data, $expected, $call)
  {
    $data = array('sub' => $data);
    $expected = array('sub' => $expected);

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
        $args[] = $functionEmulation;
        $sub = new Mongo_Subdocument($doc, 'sub');
        call_user_func_array([$sub, $method], $args);
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

