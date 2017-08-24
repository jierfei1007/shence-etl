package com.facishare.fhc.test

import java.util
import java.util.regex.Pattern

import junit.framework.TestCase

import scala.collection.mutable.ArrayBuffer
import scala.util.matching.Regex

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
    val array = new ArrayBuffer[Int]()
    array+=2
    array.foreach(println(_))
//    val b="zhangsan"
//    a.foreach(e=>fun(b,e))
//    def fun(arg1:String,arg2:Int){println(arg1+";"+arg2)}

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
    val a=Array(1,2,3,4,5)
    for( i <- 0 until(a.length)){
      println(i)
    }
    for( i <- a.indices){
      println(i)
    }
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
    a("jief")(33)
  }

  def test8(): Unit ={
    val p2="0|1|2".r
    println(p2.pattern.matcher("2").matches())
//    val p1="^-?[0-9]\\d*$".r
//    println(p1.pattern.matcher("3234").matches())
//
//    val pattern = new Regex("^-?[0-9]\\d*$")
//    val p=Pattern.compile("^-?[0-9]\\d*$")
//    println(p.matcher("231").matches())
  }

  def test9(): Unit ={
    val number_regex="^[0-9]\\d*$".r
    println(number_regex.pattern.matcher("2121312").matches())
  }
}
