package com.facishare.fhc.test

import java.util

import junit.framework.TestCase
import java.util.{Date, Map => JMap}

import scala.collection.JavaConversions._



/**
  * Created by jief on 2016/12/21.
  */
class testArray extends TestCase{

  def testArrayopt(): Unit ={
//    val CEP = "CEP"
//    var event_value = ""
//    val initNum=3
//    val array=Array("jief","yujing","zhangsan","lisi")
//    for(j<-0 until initNum){
//      event_value+="_"+array(j)
//    }
//    event_value=CEP+event_value
//    println(event_value)
    val studentInfo=Map("john" -> 21, "stephen" -> 22,"lucy" -> 20)
//    val user=new User()
//    user.setAge(11)
//    user.setName("jief")
//    println(JsonUtil.toJsonString())
  }

  def testMap():Unit={
//    var map = new java.util.HashMap[java.lang.String, Any]
    val a:Int=100
    val b:String="jief"
//    map  jkv("aaaa",a)

      val map: JMap[String, Object] =new util.HashMap[String,Object]()
      map.put("a",a.asInstanceOf[AnyRef])
      map.put("date", new Date().asInstanceOf[AnyRef])
      map.put("b",b)
      User.testMap(map)

  }
  def jkv(s: String, a: Any) = s -> a.asInstanceOf[AnyRef]

  def testCharReplace(): Unit ={
    val a:String="CEP_account_register!sendSecurityCode_2E_action"
    if(a.contains(".")){
      println("EventValue",a.replaceAll("\\.", "_2E_"))
    }else if(a.contains("-")){
      println("EventValue",a.replaceAll("-", "_2D_"))
    }else if(a.contains("?")){
      println("EventValue",a.replaceAll("\\?", "_3F_"))
    }else if(a.contains("&")){
      println("EventValue",a.replaceAll("\\&", "_26_"))
    }else if(a.contains("!")){
      println("EventValue",a.replaceAll("\\!", "_21_"))
    }else if(a.contains("=")){
      println("EventValue",a.replaceAll("=", "_26_"))
    }else {
      println("EventValue", a)
    }
  }

  def testList():Unit={
    var result = List[Int]()
    result.::(1).foreach(println(_))
  }

  def testJavaScalaMap(): Unit ={
    import java.util.{Map => JMap}
    import scala.collection.JavaConversions._
    val map: JMap[String, Int] = Map("first" -> 1, "second" -> 2)
    map.foreach(kv =>{
      println(kv._1)
    })
  }
}
