package com.facishare.fhc.main

import java.net.InetAddress
import java.util.{Date, Locale, Map => JMap}

import com.facishare.fhc.source.UserToShenceSource
import com.facishare.fhc.util.{HDFSLogFactory, HDFSUtil, SendMsgToShence}
import com.facishare.fs.cloud.helper.msg.MessageSender
import com.facishare.fs.cloud.helper.util.ParaJudge
import com.sensorsdata.analytics.javasdk.SensorsAnalytics
import org.apache.commons.lang.StringUtils
import org.apache.spark.{Accumulator, SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by jief on 2017/5/26.
  */
object UserToShenceMain {

  def main(args: Array[String]): Unit = {
    /**
      * 判断传入参数个数
      */
    val waringMsg = "XXX.jar \n" +
      " runMode(local,yarn-cluster) \n" +
      " 20161220\n" +
      " 09\n"
    val isExit = !ParaJudge.judge(args, 2, waringMsg)
    //如果参数个数错误,则直接退出
    isExit match {
      case true => return
      case _ =>
    }
    val runModel = args(0)
    val run_dt = args(1)
    //验证参数
    assert(StringUtils.isNotBlank(runModel), "runMode(local,yarn-cluster) can not be blank")
    assert(StringUtils.isNotBlank(run_dt), "date can not be blank")
    /**
      * 从配置中心获取参数
      */
    //配置spark conf
    val sparkConf = new SparkConf().setAppName("user-shence-etl").setMaster(runModel)
    val sparkContext = new SparkContext(sparkConf)
    val accumulator:Accumulator[Long] = sparkContext.accumulator(0, "add-shence-nums")
    val errorNums:Accumulator[Long] = sparkContext.accumulator(0, "error-nums")
    val hiveContext: HiveContext = new HiveContext(sparkContext)
    val user_info_RDD=UserToShenceSource.getUserInfoDF(hiveContext,run_dt)
    //save to shence
    user_info_RDD.repartition(20).foreachPartition(itor=>sendLogToShence(accumulator,errorNums,run_dt,"default",itor))
    val nums=errorNums.value
    if(nums>0){
      val msg="[仓库数据入神测] \n 用户信息导入神测失败数量:"+nums+"\n 日期:"+run_dt+"\n"+ "[负责人: 田春;魏磊;王杰朝;武靖;纪二飞;王正坤;王海利;姚致远][发送人：武靖]"
      MessageSender.sendMsg(msg,Array(998,4097,3719,6021,1368,4686,5458))
    }
    sparkContext.stop()
  }

  /**
    * 发送数据到神测服务
    *
    * @param iterator
    */
  def sendLogToShence(accumulator: Accumulator[Long],errorNums:Accumulator[Long], dt: String, projectName: String, iterator: Iterator[(String, JMap[String, Object])]): Unit = {
    //初始化hdfs报错路径
    val qixin_error_log_dir: String = com.facishare.fhc.util.Context.shence_error_log_dir + "/" + "user-to-shence" + "/" + dt + "/"
    val iAddress: InetAddress = InetAddress.getLocalHost
    val hostName: String = iAddress.getHostName
    val qixin_error_log_file: String = qixin_error_log_dir + "user-to-shence" + "_" + hostName + "_" + System.currentTimeMillis() + ".err"
    val hlog = HDFSLogFactory.getHDFSLog(qixin_error_log_file)
    val sa: SensorsAnalytics = SendMsgToShence.getSA(projectName)
    while (iterator.hasNext) {
      val cep = iterator.next()
      val map = cep._2
      try {
        SendMsgToShence.profileSet(sa, cep._1, map)
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
