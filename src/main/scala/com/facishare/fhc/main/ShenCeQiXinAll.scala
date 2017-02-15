package com.facishare.fhc.main

import java.net.InetAddress
import java.util.Map
import java.util.{Date, Map => JMap}

import com.facishare.fhc.source.QiXinSource
import com.facishare.fhc.util.{HDFSLogFactory, HDFSUtil, SendMsgToShence}
import com.facishare.fs.cloud.helper.util.ParaJudge
import com.sensorsdata.analytics.javasdk.SensorsAnalytics
import org.apache.commons.lang.StringUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by jief on 2017/1/12.
  */
object ShenCeQiXinAll {
  def main(args: Array[String]): Unit = {
    /**
      * 判断传入参数个数
      */
    val waringMsg = "XXX.jar \n" +
      " runMode(local,yarn-cluster) \n" +
      " 20161220\n" +
      " b_qx_createsession_detail \n"
    val isExit = !ParaJudge.judge(args, 3, waringMsg)
    //如果参数个数错误,则直接退出
    isExit match {
      case true => return
      case _ =>
    }
    val runModel = args(0)
    val projectName = args(1)
    val dt = args(2)

    //验证参数
    assert(StringUtils.isNotBlank(runModel), "runMode(local,yarn-cluster) can not be blank!")
    assert(StringUtils.isNotBlank(dt), "date can not be blank!")
    require(StringUtils.isNotBlank(projectName), "shence project name can not be blank!")

    val b_qx_createsession_detail="b_qx_createsession_detail"
    val b_qx_markread_session_detail="b_qx_markread_session_detail"
    val b_qx_message_general_detail="b_qx_message_general_detail"
    val b_qx_message_igt_detail="b_qx_message_igt_detail"

    val sparkConf = new SparkConf().setAppName("shence-etl-qixin").setMaster(runModel)
    val sparkContext = new SparkContext(sparkConf)
    val hiveContext: HiveContext = new HiveContext(sparkContext)

    //b_qx_createsession_detail

    val qx_csd_df = QiXinSource.getQXCreateSessionDF(hiveContext, b_qx_createsession_detail, dt)
    val qx_csd_rdd: RDD[Tuple3[String, String, JMap[String, Object]]] = QiXinSource.getQXCreateSessionEventDF(qx_csd_df, b_qx_createsession_detail)
    qx_csd_rdd.coalesce(10,false).foreachPartition(itor => sendLogToShence(b_qx_createsession_detail,dt,projectName, itor))

    //b_qx_markread_session_detail
    val qx_msd_df = QiXinSource.getQXCreateSessionDF(hiveContext, b_qx_createsession_detail, dt)
    val qx_msd_rdd: RDD[Tuple3[String, String, JMap[String, Object]]] = QiXinSource.getQXCreateSessionEventDF(qx_msd_df, b_qx_markread_session_detail)
    qx_msd_rdd.coalesce(10,false).foreachPartition(itor => sendLogToShence(b_qx_markread_session_detail,dt,projectName, itor))

    //b_qx_message_general_detail
    val qx_mgd_rdd:RDD[Tuple3[String, String, JMap[String, Object]]]= QiXinSource.getQXMessageGeneralRDD(hiveContext,dt)
    qx_mgd_rdd.coalesce(10,false).foreachPartition(itor=>sendLogToShence(b_qx_message_general_detail,dt,projectName,itor))

    //b_qx_message_igt_detail
    val qx_mid_rdd:RDD[Tuple3[String, String, JMap[String, Object]]]= QiXinSource.getQXMessageigtRDD(hiveContext,dt)
    qx_mid_rdd.coalesce(10,false).foreachPartition(itor=>sendLogToShence(b_qx_message_igt_detail,dt,projectName,itor))
    sparkContext.stop()
  }

  /**
    * 发送数据到神测服务
    *
    * @param iterator
    */
  def sendLogToShence(title:String,dt:String,projectName: String, iterator: Iterator[Tuple3[String, String, JMap[String, Object]]]): Unit = {
    //初始化hdfs报错路径
    val qixin_error_log_dir:String= com.facishare.fhc.util.Context.shence_error_log_dir+"/"+title+"/"+dt+"/"
    val iAddress: InetAddress = InetAddress.getLocalHost
    val hostName: String = iAddress.getHostName
    val qixin_error_log_file:String=qixin_error_log_dir+title+"_"+hostName+"_"+System.currentTimeMillis()+".err"
    val hlog = HDFSLogFactory.getHDFSLog(qixin_error_log_file)
    val sa: SensorsAnalytics = SendMsgToShence.getSA(projectName)
    while (iterator.hasNext) {
      val cep = iterator.next()
      val map = cep._3
      try {
        SendMsgToShence.writeLog(sa, cep._1, cep._2, map)
      }catch{
        case e:Throwable =>{
          val outputStream=hlog.getOutPutStream()
          HDFSUtil.write2File(outputStream, map.toString+"errormsg:"+e.getMessage)
        }
      }
    }
    sa.flush()
    sa.shutdown()
    hlog.close()
  }
}