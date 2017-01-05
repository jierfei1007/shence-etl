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
  def test2(): Unit ={
    val eventName="b_qx_markread_session_detail"
    eventName match {
      case "b_qx_markread_session_detail" =>{println("hahahah")}
      case _ => throw new RuntimeException("none")
    }
  }

  def testbibao(): Unit ={
    val a = Array(1,2,3,4,5)
    val b="zhangsan"
    a.foreach(e=>fun(b,e))
    def fun(arg1:String,arg2:Int){println(arg1+";"+arg2)}
  }
}
