<?php
/**
 * This json wrapper is included to provide a lenient decode method.
 *
 * @author  Colin Mollenhour
 * @package Mongo_Database
 */

class JSON {

  /**
   * Decode a JSON string that is not strictly formed.
   *
   * @param  string  $json
   * @param  boolean $assoc
   * @return array|object
   */
  public static function decode($json, $assoc = FALSE)
  {
    $json = utf8_encode($json);
    $json = str_replace(array("\n","\r"),"",$json);
    $json = preg_replace('/([{,])(\s*)([^"]+?)\s*:/','$1"$3":',$json);
    return json_decode($json,$assoc);
  }

  /**
   * Decode a JSON string that is not strictly formed into an array.
   *
   * @param  string  $json
   * @return array
   */
  public static function arr($json)
  {
    return self::decode($json,TRUE);
  }

  /**
   * Decode a JSON string that is not strictly formed into an object.
   *
   * @param  string  $json
   * @return object
   */
  public static function obj($json)
  {
    return self::decode($json);
  }

  /**
   * Encode an array or object into a JSON string
   *
   * @param  array|object $value
   * @return  string
   */
  public static function str($value)
  {
    return json_encode($value);
  }

}
