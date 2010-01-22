<?php

class JSON {

  public static function decode($json, $assoc = FALSE)
  {
    $json = utf8_encode($json);
    $json = str_replace(array("\n","\r"),"",$json);
    $json = preg_replace('/([{,])(\s*)([^"]+?)\s*:/','$1"$3":',$json);
    return json_decode($json,$assoc);
  }

  public static function arr($json)
  {
    return self::decode($json,TRUE);
  }

  public static function obj($json)
  {
    return self::decode($json);
  }

  public static function str($value)
  {
    return json_encode($value);
  }

}