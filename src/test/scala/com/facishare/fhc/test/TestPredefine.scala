package com.facishare.fhc.test

import java.util

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

  def test3(): Unit ={

    def aa(a:String)(block: =>Unit){
      block
      println(a)
    }
    def run():Unit=aa("jief")(run2())
    def run2():Unit=aa("jief"){
      println("hahahah")
    }
    run()

  }

  def test4(): Unit ={
    val a=(1,2,3,4,5)
    println(a._1)
  }

  def test5(): Unit ={
    val map = new util.HashMap[String, Object]()
    val a:Long=12345
    map.put("aaa",a.toString)
    println(map.get("aaa").getClass)
  }
  //test StringContext
  def test6(): Unit ={
    val a=123
    println(s"$a")
  }

  def test7(): Unit ={
    def a(name:String)(age:Int): Unit ={
      println("name:"+name+",age="+age)
    }
    println(a("jief"))
  }
}
