package com.facishare.fhc.main

import java.text.SimpleDateFormat
import java.util.{Date, Map => JMap}

import com.facishare.fhc.source.QiXinSource
import com.facishare.fhc.util.SendMsgToShence
import com.facishare.fs.cloud.helper.util.ParaJudge
import com.sensorsdata.analytics.javasdk.SensorsAnalytics
import org.apache.commons.lang.StringUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by jief on 2017/1/5.
  */
object ShenceQiXinMain {

  def main(args: Array[String]): Unit = {
    /**
      * 判断传入参数个数
      */
    val waringMsg = "XXX.jar \n" +
      " runMode(local,yarn-cluster) \n" +
      " 20161220\n" +
      " b_qx_createsession_detail \n"
    val isExit = !ParaJudge.judge(args, 4, waringMsg)
    //如果参数个数错误,则直接退出
    isExit match {
      case true => return
      case _ =>
    }
    val runModel = args(0)
    val projectName=args(1)
    val dt = args(2)
    val tableName=args(3)
    //验证参数
    assert(StringUtils.isNotBlank(runModel), "runMode(local,yarn-cluster) can not be blank!")
    assert(StringUtils.isNotBlank(dt), "date can not be blank!")
    require(StringUtils.isNotBlank(tableName),"tableName can not be blank!")
    require(StringUtils.isNotBlank(projectName),"shence project name can not be blank!")

    val sparkConf = new SparkConf().setAppName("shence-etl-qixin").setMaster(runModel)
    val sparkContext = new SparkContext(sparkConf)
    val hiveContext: HiveContext = new HiveContext(sparkContext)

    tableName match {
      case "b_qx_createsession_detail" | "b_qx_markread_session_detail" =>{
        val df=QiXinSource.getQXCreateSessionDF(hiveContext,tableName,dt)
        val rdd:RDD[Tuple3[String, String, JMap[String, Object]]]=QiXinSource.getQXCreateSessionEventDF(df,tableName)
        rdd.foreachPartition(itor=>sendLogToShence(projectName,itor))
      }
//      case "b_qx_markread_session_detail"=>{
//        val df=QiXinSource.getQXCreateSessionDF(hiveContext,tableName,dt)
//        val rdd:RDD[Tuple3[String, String, JMap[String, Object]]]=QiXinSource.getQXCreateSessionEventDF(df,tableName)
//        rdd.foreachPartition(itor=>sendLogToShence(projectName,itor))
//      }
      case "b_qx_message_general_detail"=>{
       val rdd:RDD[Tuple3[String, String, JMap[String, Object]]]= QiXinSource.getQXMessageGeneralRDD(hiveContext,dt)
        rdd.foreachPartition(itor=>sendLogToShence(projectName,itor))
      }
      case "b_qx_message_igt_detail" =>{
        val rdd:RDD[Tuple3[String, String, JMap[String, Object]]]= QiXinSource.getQXMessageigtRDD(hiveContext,dt)
        rdd.foreachPartition(itor=>sendLogToShence(projectName,itor))
      }
      case _ => throw new RuntimeException("no such event name")
    }
    sparkContext.stop()
  }

  /**
    * 发送数据到神测服务
    * @param iterator
    */
  def sendLogToShence(projectName:String,iterator: Iterator[Tuple3[String,String,JMap[String,Object]]]): Unit ={
    val sa: SensorsAnalytics = SendMsgToShence.getSA(projectName)
    while (iterator.hasNext) {
      val cep = iterator.next()
      val map = cep._3
      SendMsgToShence.writeLog(sa,cep._1,cep._2,map)
    }
    sa.flush()
    sa.shutdown()
  }
}
