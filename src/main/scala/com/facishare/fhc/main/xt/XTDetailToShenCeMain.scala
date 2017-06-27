package com.facishare.fhc.main.xt

import java.net.InetAddress
import java.util.{Map => JMap}

import com.facishare.fhc.source.QiXinSource
import com.facishare.fhc.source.xt.XTFeedSource
import com.facishare.fhc.util.{HDFSLogFactory, HDFSUtil, Properties, SendMsgToShence}
import com.sensorsdata.analytics.javasdk.SensorsAnalytics
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{Accumulator, SparkConf, SparkContext}

/**
  * Created by jief on 2017/6/26.
  */
object XTDetailToShenCeMain {


  def main(args: Array[String]): Unit = {
    //进行输入参数校验
    if (args.length <4 ) {
      throw new RuntimeException("args length should be 4")
    }
    //spark运行环境 ("foneshare")
    val runEnvironment = args(0)
    //spark运行模式  ("yarn-cluster")
    val runMode = args(1)
    //获取计算日期，如果转换失败则直接退出 (2016-08-14)
    val runDateStr = args(2)
    //配置中心配置文件 ("fhc-bdata-active")
    val configFileName = args(3)
    //配置中心配置组   ("foneshare")
    val configFileGroup = args(0)
    //读取配置中心
    val propConfig:java.util.Map[String, String] = Properties.setProperty(configFileName, configFileGroup)
    val sparkConf = new SparkConf().setAppName("发送协同数据到神策").setMaster(runMode)
    val sparkContext = new SparkContext(sparkConf)
    val hiveContext = new HiveContext(sparkContext)
    val partition=runDateStr.replace("-","")
    val accumulator:Accumulator[Long] = sparkContext.accumulator(0, "add-shence-nums")
    val errorNums:Accumulator[Long] = sparkContext.accumulator(0, "error-nums")
    //
    val fr_rdd: RDD[(String, String, JMap[String, Object])] = XTFeedSource.createFeedReplyDF(hiveContext,partition,propConfig)
    fr_rdd.coalesce(10,true).foreachPartition(itor => sendLogToShence(accumulator,errorNums,"b_xt_sendfeedreply_detail", "partition", "default", itor))
    val sr_rdd: RDD[(String, String, JMap[String, Object])] = XTFeedSource.createSendReceipt(hiveContext,partition,propConfig)
    sr_rdd.coalesce(10,true).foreachPartition(itor => sendLogToShence(accumulator,errorNums,"b_xt_sentreceipt_detail", "partition", "default", itor))
    val fl_rdd: RDD[(String, String, JMap[String, Object])] = XTFeedSource.createFeedLike(hiveContext,partition,propConfig)
    fl_rdd.coalesce(10,true).foreachPartition(itor => sendLogToShence(accumulator,errorNums,"b_xt_feedlike_detail", "partition", "default", itor))
    sparkContext.stop()
  }


  /**
    * 发送数据到神测服务
    *
    * @param iterator
    */
  def sendLogToShence(accumulator: Accumulator[Long],errorNums:Accumulator[Long],title:String,dt:String,projectName: String, iterator: Iterator[(String, String, JMap[String, Object])]): Unit = {
    //初始化hdfs报错路径
    val xt_error_log_dir:String= com.facishare.fhc.util.Context.shence_error_log_dir+"/"+title+"/"+dt+"/"
    val iAddress: InetAddress = InetAddress.getLocalHost
    val hostName: String = iAddress.getHostName
    val qixin_error_log_file:String=xt_error_log_dir+title+"_"+hostName+"_"+System.currentTimeMillis()+".err"
    val hlog = HDFSLogFactory.getHDFSLog(qixin_error_log_file)
    val sa: SensorsAnalytics = SendMsgToShence.getSA(projectName)
    while (iterator.hasNext) {
      val cep = iterator.next()
      val map = cep._3
      try {
        SendMsgToShence.writeLog(sa, cep._1, cep._2, map)
        accumulator.add(1L)
      }catch{
        case e:Exception =>{
          errorNums.add(1L)
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
