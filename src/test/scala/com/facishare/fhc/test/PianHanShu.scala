package com.facishare.fhc.test
import java.util.Date
/**
  * Created by jief on 2017/1/4.
  */
object PianHanShu {

  def main(args: Array[String]): Unit = {
//    val date = new Date
//    log(date, "message1" )
//    Thread.sleep(1000)
//    log(date, "message2" )
//    Thread.sleep(1000)
//    log(date, "message3" )

    val date = new Date
    val logWithDateBound = log(date, _ : String)
    logWithDateBound("message1" )
    Thread.sleep(1000)
    logWithDateBound("message2" )
    Thread.sleep(1000)
    logWithDateBound("message3" )
  }
  def log(date: Date, message: String)  = {
    println(date + "----" + message)
  }
}
