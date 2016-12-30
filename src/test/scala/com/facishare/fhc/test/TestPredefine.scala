package com.facishare.fhc.test

import junit.framework.TestCase

/**
  * Created by jief on 2016/12/28.
  */
class TestPredefine extends TestCase{

  def test1(): Unit ={
    val c : Char = 97.asInstanceOf[Char]
    "hello".asInstanceOf[String]
    1.asInstanceOf[Long]
    val it: Seq[String] = List("a", "b")
    it.asInstanceOf[List[String]]
    println("hello".isInstanceOf[String])
    println(classOf[String])
  }
}
