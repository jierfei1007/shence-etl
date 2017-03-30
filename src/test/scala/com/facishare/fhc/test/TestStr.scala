package com.facishare.fhc.test

import java.io.{BufferedReader, File, FileInputStream, InputStreamReader}

import junit.framework.TestCase

import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.util.matching.Regex

/**
  * Created by jief on 2016/12/21.
  */
class TestStr extends TestCase {

  def testregx(): Unit = {
    var uri: String = "/DemoAccount/958165@"
    uri = "/Dump.aspx"
    uri = "/H/Approve/SendReply"
    uri = "/StaticFiles/"
    //uri="/StaticFiles/喇喇/WEB-INF/web.xml"
//    uri="/WebReg/GetAreaCode.orig"
    //uri="/a/android/Account02/sendMobileBindingSMSCode"
    //uri="/a/iphone/Feed/Reply"
    uri="/scripts/openflashchart/open-flash-chart/php-ofc-library/ofc_upload_image.php/sssss/ssss/sss"
    //uri="FHE/EM1AAV/AVConversation/UpdateMultiAVConversation"
    //uri="FHE/EM1HTrain/courseMobCtrl/approveCourse"
    //uri="FRL/WebReg/Register/Init"
    //uri="XV/Invite/SendSMSCode"
    val CEP = "CEP_"
    var event_value = ""
    var firstActionName = ""
    var secondActionName = ""
    var lastActionName = ""

    val regex = new Regex("""^/(\w|\W)+""")
    if (regex.pattern.matcher(uri).matches()) {
      uri = uri.substring(1)
    }
    val action_array: Array[String] = uri.split('/')

    println(mkevents(action_array))

  }


  def mkevents(actionArray:Array[String]):Tuple4[String,String,String,String]= {
    val CEP = "CEP"
    var event_value = ""
    var firstActionName = ""
    var secondActionName = ""
    var lastActionName = ""

    actionArray.size match {
      case 1 =>{
        event_value = CEP + "_"+ actionArray.mkString("_")
      }
      case 2 =>{
        event_value = CEP + "_"+ actionArray.mkString("_")
      }
      case 3 =>{
        if(actionArray(0).equalsIgnoreCase("a")){
          event_value = CEP + "_"+ actionArray.mkString("_")
        }else{
          event_value = CEP +"_"+ actionArray(0)+"_"+actionArray(1)
          lastActionName=actionArray(2)
        }
      }
      case 4 =>{
        if(actionArray(0).equalsIgnoreCase("a")){
          event_value = CEP +"_"+ actionArray(0)+"_"+actionArray(1)+"_"+actionArray(2)
          lastActionName=actionArray(3)
        }else{
          event_value = CEP +"_"+ actionArray(0)+"_"+actionArray(1)
          firstActionName=event_value+"_"+actionArray(2)
          lastActionName=actionArray(3)
        }
      }
      case _=>{
        event_value = CEP +"_"+ actionArray(0)+"_"+actionArray(1)
        firstActionName=event_value+"_"+actionArray(2)
        secondActionName=firstActionName+"_"+actionArray(3)
        lastActionName=actionArray(actionArray.size-1)

      }
    }
    (event_value, firstActionName, secondActionName, lastActionName)
  }

  def test4(): Unit ={

    def join(name:String)=(age:Int)=>{
      println(name+":"+age)
    }
   val array=Array(1,2,3,4,5,6)
    array.foreach(join("jief"))
  }

  def test5(): Unit ={
    val a="b_qx_markread_session_detail"
    a match {
      case "b_qx_createsession_detail" | "b_qx_markread_session_detail" =>{println("hhhhh")}
      case _ => {println("none")}
    }
  }

  def test6(): Unit ={

    def get:String={
      "jief"
    }
    val name={
      get
    }
    println("name="+name)

  }
  def test7(): Unit ={
    val date= new java.util.Date(1484299221385L)
    println(date)
  }

  def test8(): Unit ={
    var a=ListBuffer[Tuple2[Int,Int]]()
    a.+=((1,2))
    var b=ListBuffer[Tuple2[Int,Int]]()
    b.+=((3,4))
    a.++(b)
  }

  def test9(): Unit ={
    val a = ListBuffer[Tuple2[Int,Int]](Tuple2(1,2))
    val b = ListBuffer[Tuple2[Int,Int]](Tuple2(3,4))
    val c=a++ b
    c.foreach(kv=>println(kv._1+"----"+kv._2))
  }
  def test10(): Unit ={
    val a="5.4.0"
    val b="5.3.9.9.9"
    println(a>b)
  }

  def test11(): Unit ={
    val product_version_regex="\\d+(\\.\\d+)*".r
    println(product_version_regex.pattern.matcher("12.3.3.3").matches())
  }

  def test12(): Unit ={
     def a(name:String):String = bb{
       println(name)
       "jief"
     }
    def bb(a:String):String={
      println("aaa===="+a)
      a*2
    }
    a("dsjfls")
  }

  def test13(): Unit ={

    def b:String={
      println("sss")
      "bbb"
    }
    def a:String={
       "jief"
     }

    println(a.length)

  }

  def test14(): Unit ={

    val a=(name:String)=>println(name)
    a("jief")
  }

  def test15(): Unit ={
    val referenceBuffer = new ArrayBuffer[String]
    referenceBuffer +="jief"
    referenceBuffer +="jief"
    referenceBuffer +="hhh"
    referenceBuffer.foreach(println(_))
    referenceBuffer -="jief"
    referenceBuffer.foreach(println(_))
  }

  def test16(): Unit ={
    val params="20170303,12"
    var sql="select * from a where dt='%s' and hr='%s'"

    sql=sql.format()
    println(sql)
  }

  def test17(): Unit ={

    def logexec[T](f: => T): T ={
      f
    }
    def run()=logexec{
      println("hahaha")
    }
    run()

  }
  def testReadFile(): Unit ={
    val file = new File("D://财务留存.sql")
    val inputStream=new FileInputStream(file)
    val bufferReader = new BufferedReader(new InputStreamReader(inputStream))
    var txt=""
    var line:String=bufferReader.readLine()
    while (line !=null) {
      txt=txt+line+"\n"
      line=bufferReader.readLine()
    }
    println(txt)
    inputStream.close()
  }


  def testPinggai(): Unit ={
    var pingzi=5
    var gaizi=5
    def huan(tuple2: Tuple2[Int,Int]):Tuple2[Int,Int]={


     null
    }

  }
}
