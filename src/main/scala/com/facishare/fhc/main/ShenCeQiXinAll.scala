package com.facishare.fhc.main

import java.net.InetAddress
import java.util
import java.util.Map
import java.util.{Date, Map => JMap}

import com.facishare.fhc.source.QiXinSource
import com.facishare.fhc.util.{HDFSLogFactory, HDFSUtil, SendMsgToShence}
import com.facishare.fs.cloud.helper.msg.MessageSender
import com.facishare.fs.cloud.helper.util.ParaJudge
import com.fxiaoke.dataplatform.utils.alarm.ServiceNumAlarm
import com.sensorsdata.analytics.javasdk.SensorsAnalytics
import org.apache.commons.lang.StringUtils
import org.apache.spark.{Accumulator, SparkConf, SparkContext}
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
    val b_qx_session_set_detail="b_qx_session_set_detail"
    val b_qx_markread_session_detail="b_qx_markread_session_detail"
    val b_qx_message_general_detail="b_qx_message_general_detail"
    val b_qx_message_igt_detail="b_qx_message_igt_detail"

    val sparkConf = new SparkConf().setAppName("shence-etl-qixin").setMaster(runModel)
    val sparkContext = new SparkContext(sparkConf)
    val accumulator:Accumulator[Long] = sparkContext.accumulator(0, "add-shence-nums")
    val errorNums:Accumulator[Long] = sparkContext.accumulator(0, "error-nums")
    val hiveContext: HiveContext = new HiveContext(sparkContext)

    //b_qx_createsession_detail

    val qx_csd_df = QiXinSource.getQXCreateSessionDF(hiveContext, b_qx_createsession_detail, dt)
    val qx_csd_rdd: RDD[Tuple3[String, String, JMap[String, Object]]] = QiXinSource.getQXCreateSessionEventDF(qx_csd_df, b_qx_createsession_detail)
    qx_csd_rdd.coalesce(10,false).foreachPartition(itor => sendLogToShence(accumulator,errorNums,b_qx_createsession_detail,dt,projectName, itor))
    //b_qx_session_set_detail
    val qx_ssd_df = QiXinSource.getQXCreateSessionDF(hiveContext, b_qx_session_set_detail, dt)
    //由于b_qx_session_set_detail 为新加事件所以此表中的数据也用b_qx_createsession_detail事件名称发送到神策
    val qx_ssd_rdd: RDD[Tuple3[String, String, JMap[String, Object]]] = QiXinSource.getQXCreateSessionEventDF(qx_ssd_df, b_qx_createsession_detail)
    qx_ssd_rdd.coalesce(10,false).foreachPartition(itor => sendLogToShence(accumulator,errorNums,b_qx_session_set_detail,dt,projectName, itor))
    //b_qx_markread_session_detail
    val qx_msd_df = QiXinSource.getQXCreateSessionDF(hiveContext, b_qx_createsession_detail, dt)
    val qx_msd_rdd: RDD[Tuple3[String, String, JMap[String, Object]]] = QiXinSource.getQXCreateSessionEventDF(qx_msd_df, b_qx_markread_session_detail)
    qx_msd_rdd.coalesce(10,false).foreachPartition(itor => sendLogToShence(accumulator,errorNums,b_qx_markread_session_detail,dt,projectName, itor))

    //b_qx_message_general_detail
    val qx_mgd_rdd:RDD[Tuple3[String, String, JMap[String, Object]]]= QiXinSource.getQXMessageGeneralRDD(hiveContext,dt)
    qx_mgd_rdd.coalesce(10,false).foreachPartition(itor=>sendLogToShence(accumulator,errorNums,b_qx_message_general_detail,dt,projectName,itor))

    //b_qx_message_igt_detail
    val qx_mid_rdd:RDD[Tuple3[String, String, JMap[String, Object]]]= QiXinSource.getQXMessageigtRDD(hiveContext,dt)
    qx_mid_rdd.coalesce(10,false).foreachPartition(itor=>sendLogToShence(accumulator,errorNums,b_qx_message_igt_detail,dt,projectName,itor))
    //发送报警
    val nums=errorNums.value
    if(nums>0){
      val msg="[仓库数据入神测] \n事件类型：企信 \t错误数:"+nums+"\n"+"日期:"+dt+"\n[负责人: 武靖;纪二飞;王正坤;宫殿][发送人：武靖]"
      val list=List[String]("4998","4097","3719","6021","1368","4686","5458")
      val Jlist=new util.ArrayList[String]()
      list.foreach(e=>Jlist.add(e))
      new ServiceNumAlarm().sendAlarm(msg.toString,"FSAID_5f5e554",Jlist)
    }
    sparkContext.stop()
  }

  /**
    * 发送数据到神测服务
    *
    * @param iterator
    */
  def sendLogToShence(accumulator: Accumulator[Long],errorNums:Accumulator[Long],title:String,dt:String,projectName: String, iterator: Iterator[Tuple3[String, String, JMap[String, Object]]]): Unit = {
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