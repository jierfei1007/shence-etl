package com.facishare.fhc.main

import java.net.InetAddress
import java.text.SimpleDateFormat
import java.util.{Date, Map => JMap}

import com.facishare.fhc.source.QiXinSource
import com.facishare.fhc.util.{HDFSLogFactory, HDFSUtil, SendMsgToShence}
import com.facishare.fs.cloud.helper.msg.MessageSender
import com.facishare.fs.cloud.helper.util.ParaJudge
import com.sensorsdata.analytics.javasdk.SensorsAnalytics
import org.apache.commons.lang.StringUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{Accumulator, SparkConf, SparkContext}

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
    val projectName = args(1)
    val dt = args(2)
    val tableName = args(3)
    //验证参数
    assert(StringUtils.isNotBlank(runModel), "runMode(local,yarn-cluster) can not be blank!")
    assert(StringUtils.isNotBlank(dt), "date can not be blank!")
    require(StringUtils.isNotBlank(tableName), "tableName can not be blank!")
    require(StringUtils.isNotBlank(projectName), "shence project name can not be blank!")

    val sparkConf = new SparkConf().setAppName("shence-etl-qixin").setMaster(runModel)
    val sparkContext = new SparkContext(sparkConf)
    val accumulator:Accumulator[Long] = sparkContext.accumulator(0, "add-shence-nums")
    val errorNums:Accumulator[Long] = sparkContext.accumulator(0, "error-nums")
    val hiveContext: HiveContext = new HiveContext(sparkContext)

    tableName match {
      case "b_qx_createsession_detail" | "b_qx_markread_session_detail" => {
        val df = QiXinSource.getQXCreateSessionDF(hiveContext, tableName, dt)
        val rdd: RDD[Tuple3[String, String, JMap[String, Object]]] = QiXinSource.getQXCreateSessionEventDF(df, tableName)
        rdd.foreachPartition(itor => sendLogToShence(accumulator,errorNums,tableName, dt, projectName, itor))
      }
      case "b_qx_message_general_detail" => {
        val rdd: RDD[Tuple3[String, String, JMap[String, Object]]] = QiXinSource.getQXMessageGeneralRDD(hiveContext, dt)
        rdd.foreachPartition(itor => sendLogToShence(accumulator,errorNums,tableName, dt, projectName, itor))
      }
      case "b_qx_message_igt_detail" => {
        val rdd: RDD[Tuple3[String, String, JMap[String, Object]]] = QiXinSource.getQXMessageigtRDD(hiveContext, dt)
        rdd.foreachPartition(itor => sendLogToShence(accumulator,errorNums,tableName, dt, projectName, itor))
      }
      case _ => throw new RuntimeException("no such event name")
    }
    //发送报警消息
    val nums=errorNums.value
    if(nums>0){
      val msg="qixin to shence by hour error numbers is:"+nums+"\n"+"dt:"+dt
      MessageSender.sendMsg(msg,Array(4097,3719,6021,1368))
    }
    sparkContext.stop()
  }

  /**
    * 发送数据到神测服务
    *
    * @param iterator
    */
  def sendLogToShence(accumulator: Accumulator[Long],errorNums:Accumulator[Long],title: String, dt: String, projectName: String, iterator: Iterator[Tuple3[String, String, JMap[String, Object]]]): Unit = {
    //初始化hdfs报错路径
    val qixin_error_log_dir: String = com.facishare.fhc.util.Context.shence_error_log_dir + "/" + title + "/" + dt + "/"
    val iAddress: InetAddress = InetAddress.getLocalHost
    val hostName: String = iAddress.getHostName
    val qixin_error_log_file: String = qixin_error_log_dir + title + "_" + hostName + "_" + System.currentTimeMillis() + ".err"
    val hlog = HDFSLogFactory.getHDFSLog(qixin_error_log_file)
    val sa: SensorsAnalytics = SendMsgToShence.getSA(projectName)
    while (iterator.hasNext) {
      val cep = iterator.next()
      val map = cep._3
      try {
        SendMsgToShence.writeLog(sa, cep._1, cep._2, map)
        accumulator.add(1L)
      } catch {
        case e: Exception => {
          errorNums.add(1L)
          val outputStream=hlog.getOutPutStream()
          HDFSUtil.write2File(outputStream, map.toString + "errormsg:" + e.getMessage)
        }
      }
    }
    sa.flush()
    sa.shutdown()
    hlog.close()
  }
}
