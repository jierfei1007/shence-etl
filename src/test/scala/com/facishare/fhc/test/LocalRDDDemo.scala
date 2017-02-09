package com.facishare.fhc.test

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by jief on 2017/1/22.
  */
object LocalRDDDemo {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SimpleApp").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    case class Foobar(foo: String, bar: Integer)

    val foobarRdd = sc.parallelize(Array(("foo", 1),("bar", 2),("baz", -1))).toDF("name","age")
//      map { case (foo, bar) => Foobar(foo, bar) }

//    val foobarDf = foobarRdd.toDF()
//    foobarDf.limit(1).show

    foobarRdd.show()
  }

}
