package com.facishare.fhc.main

import java.net.InetAddress
import java.util.{Date, Map => JMap}

import com.facishare.fhc.source.UserLoginSource
import com.facishare.fhc.util.{HDFSLogFactory, HDFSUtil, SendMsgToShence}
import com.facishare.fs.cloud.helper.msg.MessageSender
import com.facishare.fs.cloud.helper.util.ParaJudge
import com.sensorsdata.analytics.javasdk.SensorsAnalytics
import org.apache.commons.lang.StringUtils
import org.apache.spark.{Accumulator, SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by jief on 2017/2/23.
  */
object ShenCeUserLoginByDayMain {

  def main(args: Array[String]): Unit = {
    /**
      * 判断传入参数个数
      */
    val waringMsg = "XXX.jar \n" +
      " runMode(local,yarn-cluster) \n" +
      " 20161220\n"+
      " 0"
    val isExit = !ParaJudge.judge(args, 3, waringMsg)
    //如果参数个数错误,则直接退出
    isExit match {
      case true => return
      case _ =>
    }
    val runModel = args(0)
    val dt = args(1)
    val tables=args(2)
    //验证参数
    assert(StringUtils.isNotBlank(runModel), "runMode(local,yarn-cluster) can not be blank")
    assert(StringUtils.isNotBlank(dt), "date can not be blank")
    assert(StringUtils.isNotBlank(tables) && "0|1|2".r.pattern.matcher(tables).matches(), "tables can not be blank 0:all table ,1:userLogin ,2:userLoginStatistic")
    /**
      * 从配置中心获取参数
      */
    //配置spark conf
    val sparkConf = new SparkConf().setAppName("shence-etl-userLogin").setMaster(runModel)
    val sparkContext = new SparkContext(sparkConf)
    val accumulator:Accumulator[Long] = sparkContext.accumulator(0, "add-shence-nums")
    val errorNums:Accumulator[Long] = sparkContext.accumulator(0, "error-nums")
    val hiveContext: HiveContext = new HiveContext(sparkContext)
    if("0".equals(tables)) {
      val userLoginRDD = UserLoginSource.getUserLoginDF(hiveContext, dt)
      userLoginRDD.coalesce(12).foreachPartition(itor => sendLogToShence(accumulator, errorNums, "userLogin", dt, "default", itor))
      val userLoginStatistic = UserLoginSource.getUserLoginStatic(hiveContext)
      userLoginStatistic.foreachPartition(itor => sendLogToShence(accumulator, errorNums, "userLoginStatistic", dt, "default", itor))
    }else if("1".equals(tables)){
      val userLoginRDD = UserLoginSource.getUserLoginDF(hiveContext, dt)
      userLoginRDD.coalesce(12).foreachPartition(itor => sendLogToShence(accumulator, errorNums, "userLogin", dt, "default", itor))
    }else if("2".equals(tables)){
      val userLoginStatistic = UserLoginSource.getUserLoginStatic(hiveContext)
      userLoginStatistic.foreachPartition(itor => sendLogToShence(accumulator, errorNums, "userLoginStatistic", dt, "default", itor))
    }
    val nums=errorNums.value
    if(nums>0){
      val msg = "[仓库数据入神测] \n userlogin to shence by day error numbers is:"+nums+"\n"+
      "dt:"+dt+"\n[负责人: 武靖;纪二飞;王正坤;宫殿][发送人：武靖]"
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
