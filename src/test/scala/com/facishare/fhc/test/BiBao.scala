package com.facishare.fhc.test

/**
  * Created by jief on 2017/1/4.
  */
object BiBao extends App{
//  var factor = 3
//  val multiplier = (i:Int) => i * factor
//  println( "muliplier(1) value = " +  multiplier(1) )
//  println( "muliplier(2) value = " +  multiplier(2) )

  val myMap: Map[String, String] = Map("key1" -> "value")
  val value1: Option[String] = myMap.get("key1")
  val value2: Option[String] = myMap.get("key2")

  if (value1.isDefined) {
    println("length:" + value1.get.length)
  }

  if (value2.isDefined) {
    println("length:" + value2.get.length)
  }
//  println(value1.get) // Some("value1")
//  println(value2.get) // None

  value1 match {
    case Some(content) => println("length:" + content.length)
    case None => // 啥都不做
  }

  value2 match {
    case Some(content) => println("length:" + content.length)
    case None => // 啥都不做
  }

  for (content <- value1) {
    println("length:" + content.length)
  }

  for (content <- value2) {
    println("length:" + content.length)
  }

  for (value1 <- value1; value2 <- value2) {
    println("Value1:" + value1)
    println("Value2:" + value2)
  }


  val v1 = value1.getOrElse("123") // 这个 value1 = 123
  val v2 = value2.getOrElse("0") // 这个 value2 = 0
}
