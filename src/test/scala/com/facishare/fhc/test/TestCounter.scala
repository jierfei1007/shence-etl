package com.facishare.fhc.test
import org.apache.spark.{SparkContext, SparkConf}
/**
  * Created by jief on 2016/12/28.
  */
object TestCounter {
  def main(args :Array[String]): Unit = {
    //创建一个scala版本的SparkContext
    val conf = new SparkConf().setAppName("testCounter").setMaster("local")
    val sc = new SparkContext(conf)
    val file=sc.textFile("file:///D:/file.txt")
    val blankLines =sc.accumulator(0)//创建Accumulator[Int]并初始化为0
    val callSigns =file.flatMap(line => {
      if(line == ""){
        blankLines += 1 //累加器加一
      }
      line.split(" ")
    })
    callSigns.saveAsTextFile("file:///D:/output.txt")
    println(blankLines.value)
  }
}
