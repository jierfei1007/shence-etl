package com.facishare.fhc.test

import java.text.SimpleDateFormat
import java.time.{LocalDateTime, ZoneId, ZoneOffset, ZonedDateTime}
import java.time.format.DateTimeFormatter
import java.util

import junit.framework.TestCase
import java.util.{Date, Locale, Map => JMap}

import com.sensorsdata.analytics.javasdk.SensorsAnalytics
import org.apache.spark.rdd.JdbcPartition

import scala.collection.JavaConversions._



/**
  * Created by jief on 2016/12/21.
  */
class testArray extends TestCase {

  def testArrayopt(): Unit = {
    //    val CEP = "CEP"
    //    var event_value = ""
    //    val initNum=3
    //    val array=Array("jief","yujing","zhangsan","lisi")
    //    for(j<-0 until initNum){
    //      event_value+="_"+array(j)
    //    }
    //    event_value=CEP+event_value
    //    println(event_value)
    val studentInfo = Map("john" -> 21, "stephen" -> 22, "lucy" -> 20)
    //    val user=new User()
    //    user.setAge(11)
    //    user.setName("jief")
    //    println(JsonUtil.toJsonString())
  }

  def testMap(): Unit = {
    //    var map = new java.util.HashMap[java.lang.String, Any]
    val a: Int = 100
    val b: String = "jief"
    //    map  jkv("aaaa",a)

    val map: JMap[String, Object] = new util.HashMap[String, Object]()
    map.put("a", a.asInstanceOf[AnyRef])
    map.put("date", new Date().asInstanceOf[AnyRef])
    map.put("b", b)
    User.testMap(map)

  }

  def jkv(s: String, a: Any) = s -> a.asInstanceOf[AnyRef]

  def testCharReplace(): Unit = {
    val a: String = "CEP_account_register!sendSecurityCode_2E_action"
    if (a.contains(".")) {
      println("EventValue", a.replaceAll("\\.", "_2E_"))
    } else if (a.contains("-")) {
      println("EventValue", a.replaceAll("-", "_2D_"))
    } else if (a.contains("?")) {
      println("EventValue", a.replaceAll("\\?", "_3F_"))
    } else if (a.contains("&")) {
      println("EventValue", a.replaceAll("\\&", "_26_"))
    } else if (a.contains("!")) {
      println("EventValue", a.replaceAll("\\!", "_21_"))
    } else if (a.contains("=")) {
      println("EventValue", a.replaceAll("=", "_26_"))
    } else {
      println("EventValue", a)
    }
  }

  def testList(): Unit = {
    var result = List[Int]()
    result.::(1).foreach(println(_))
  }

  def testJavaScalaMap(): Unit = {
    import java.util.{Map => JMap}
    import scala.collection.JavaConversions._
    val map: JMap[String, Int] = Map("first" -> 1, "second" -> 2)
    map.foreach(kv => {
      println(kv._1)
    })
  }

  def testpartitions(): Unit = {
    // bounds are inclusive, hence the + 1 here and - 1 on end
    val upperBound = 4
    val lowerBound = 0
    val numPartitions = 1
    val length = BigInt(1) + upperBound - lowerBound
    (0 until numPartitions).map(i => {
      val start = lowerBound + ((i * length) / numPartitions)
      val end = lowerBound + (((i + 1) * length) / numPartitions) - 1
      println(s"$i,$start,$end")
      (i, start.toLong, end.toLong)
    })
  }

  def testforeach(): Unit = {
    //    val a=Array(1,2,3,4,5,6,3)
    //    a.foreach(_=>println(_))

    //    println(new Date())

    val b ="""ldfjdauj hfdafd%sdefdf %s"""
    println(b.format("=", "+"))
  }

  def testdateTime(): Unit = {
    val a = "Tue May 14 09:10:53 CST 2013"
    val format = new SimpleDateFormat("EEE MMM dd HH:mm:ss zzz yyyy", Locale.US)
    println(format.parse(a).getTime)
    val formatter = DateTimeFormatter.ofPattern("EEE MMM dd HH:mm:ss zzz yyyy", Locale.US)
    println(LocalDateTime.parse(a, formatter).toInstant(ZoneOffset.of("+08:00")).getEpochSecond)


    //    val zoned5 = ZonedDateTime.parse(a, DateTimeFormatter.ISO_OFFSET_DATE_TIME)
    //    val local5 = zoned5.withZoneSameInstant(ZoneId.systemDefault()).toLocalDateTime()
    //    println(local5)
  }

  def testDeleteUserInfo(): Unit = {
    val sa = new SensorsAnalytics(new SensorsAnalytics.BatchConsumer("http://sasdata.foneshare.cn/sa?project=default", 100))
    sa.profileDelete("1593f42c5861ff-0d79c1d4c9a12e-6337037f-2073600-1593f42c5882b8", false)
    sa.profileDelete("15949d2c6d312a-0c5a8d3c8cabca-1d326f50-2073600-15949d2c6d6189", false)
    sa.flush()
    sa.shutdown()
  }
}
