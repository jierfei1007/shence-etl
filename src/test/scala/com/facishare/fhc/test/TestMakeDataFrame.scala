package com.facishare.fhc.test

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
  * Created by jief on 2017/2/10.
  */
object TestMakeDataFrame {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("test").setMaster("local")
    val sparkContext = new SparkContext(sparkConf)
    val sqlContext: SQLContext = new SQLContext(sparkContext)
    val enterpriseStr="1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16"
    val schema = StructType(
      List(
        StructField("enterprise_id", StringType, true)
      )
    )
    val list = enterpriseStr.split(",")
    val enterpriseIDSRDD = sparkContext.parallelize(list).map(r => {
      Row(r)
    })
    val enterpriseIDSDF = sqlContext.createDataFrame(enterpriseIDSRDD, schema)
    enterpriseIDSDF.foreachPartition(itor=>{
      while (itor.hasNext){
        val row=itor.next()
        println(row.getString(0))
      }
    })
  }

}
