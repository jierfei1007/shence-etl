package com.facishare.fhc.main

import java.net.InetAddress
import java.text.SimpleDateFormat
import java.util
import java.util.{Date, Map => JMap}
import com.facishare.fhc.bean.ShenceCEPServerAction
import com.facishare.fhc.source.CEPServerActionSource
import com.facishare.fhc.util.{HDFSUtil, JsonUtil, SendMsgToShence}
import com.facishare.fs.cloud.helper.util.ParaJudge
import com.sensorsdata.analytics.javasdk.SensorsAnalytics
import org.apache.commons.lang.StringUtils
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.matching.Regex

/**
  * <p>神测主类</p>
  * Created by jief on 2016/12/20.
  */
object ShenCeETLMain {

  def main(args: Array[String]): Unit = {
    /**
      * 判断传入参数个数
      */
    val waringMsg = "XXX.jar \n" +
      " runMode(local,yarn-cluster) \n" +
      " 20161220\n" +
      " 09\n"
    val isExit = !ParaJudge.judge(args, 3, waringMsg)
    val df:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    //如果参数个数错误,则直接退出
    isExit match {
      case true => return
      case _ =>
    }
    val runModel = args(0)
    val dt = args(1)
    val hr = args(2)
    //验证参数
    assert(StringUtils.isNotBlank(runModel), "runMode(local,yarn-cluster) can not be blank")
    assert(StringUtils.isNotBlank(dt), "date can not be blank")
    assert(StringUtils.isNotBlank(hr), "hour can not be blank")
    /**
      * 从配置中心获取参数
      */
//    val propConfig = Config.getConfig(configName, runEnvironment)
    //配置spark conf
    val sparkConf = new SparkConf().setAppName("shence-etl").setMaster(runModel)
    val sparkContext = new SparkContext(sparkConf)
    val hiveContext: HiveContext = new HiveContext(sparkContext)
    //获取cep server action DataFrame
    val cepDF = CEPServerActionSource.getCEPServerActionDF(hiveContext, dt, hr)
    val cepServerActionBean: RDD[Tuple3[Int,String,JMap[String,Object]]] = cepDF.map(row => {
      val map= new util.HashMap[String,Object]()
      val action = row.getString(0)
      val platform = row.getInt(1)
      val device_id = row.getString(2)
      val employee_ip = row.getString(3)
      val visit_time = row.getTimestamp(4)
      val duration = row.getInt(5)
      val inner_pro_version = row.getString(6)
      val eid = row.getInt(7)
      val employee_id = row.getInt(8)
      val service_type = row.getInt(9)
      val os_version = row.getString(10)
      val browser_version = row.getString(11)
//      val _time = row.getTimestamp(12)
      val browser = row.getString(13)
      val version_name = row.getString(14)
      val actions_tuple = getEventValue(action)
      //特殊字符用ascii码16进制符号替换
      var action_value:String=actions_tuple._1
      if(action_value.contains(".")){
        action_value=action_value.replaceAll("\\.", "_2E_")
      }
      if(action_value.contains("-")){
        action_value=action_value.replaceAll("-", "_2D_")
      }
      if(action_value.contains("?")){
        action_value=action_value.replaceAll("\\?", "_3F_")
      }
      if(action_value.contains("&")){
        action_value=action_value.replaceAll("\\&", "_26_")
      }
      if(action_value.contains("=")){
        action_value=action_value.replaceAll("=", "_3D_")
      }
      if(action_value.contains("!")){
        action_value=action_value.replaceAll("\\!", "_21_")
      }
      if(action_value.contains("#")){
        action_value=action_value.replaceAll("#", "_23_")
      }
      if(action_value.contains(";")){
        action_value=action_value.replaceAll(";", "_3B_")
      }
      map.put("EventValue", action_value)
      map.put("Platform", platform.asInstanceOf[AnyRef])
      map.put("DeviceID", device_id)
      map.put("IP",employee_ip)
      map.put("Time",new Date(visit_time.getTime))
      map.put("Duration", duration.asInstanceOf[AnyRef])
      map.put("ProductVersion",inner_pro_version)
      map.put("EnterpriseID", eid.asInstanceOf[AnyRef])
      map.put("UserID", employee_id.asInstanceOf[AnyRef])
      map.put("ServiceType", service_type.asInstanceOf[AnyRef])
      map.put("OSVersion",os_version)
      map.put("BrowserVersion",browser_version)
      map.put("Browser",browser)
      map.put("FullAction",action)
      map.put("FirstActionName",actions_tuple._2)
      map.put("SecondActionName",actions_tuple._3)
      map.put("LastActionName",actions_tuple._4)
      map.put("VersionName",version_name)
      map.put("FullUserID",eid.toString+"_"+employee_id.toString)
      (eid,actions_tuple._1,map)
    })
    //save to shence
    cepServerActionBean.foreachPartition(itor=>sendLogToShence(itor))
    sparkContext.stop()
  }

  /**
    * <p>写入hdfs</p>
    * @param rdd
    */
  def writeJsonToHdfs(rdd:RDD[ShenceCEPServerAction],outPut:String):Unit={
      rdd.foreachPartition(iterator => {
        val iAddress: InetAddress = InetAddress.getLocalHost
        val hostName: String = iAddress.getHostName
        val fileName: String = hostName + "_" + System.currentTimeMillis
        val hdfs_path=outPut+"/"+fileName
        val fileOut=HDFSUtil.getOutPutStream(hdfs_path)
        while (iterator.hasNext) {
          val cep = iterator.next()
          val json_cep = JsonUtil.toJsonString(cep)
          HDFSUtil.write2File(fileOut,json_cep)
        }
      })
  }
  /**
    * 发送数据到神测服务
    * @param iterator
    */
  def sendLogToShence(iterator: Iterator[Tuple3[Int,String,JMap[String,Object]]]): Unit ={
    SendMsgToShence.setProvInfo()
    val sa: SensorsAnalytics = new SensorsAnalytics(new SensorsAnalytics.BatchConsumer("http://172.17.43.58:8106/sa?project=default", 200))
    while (iterator.hasNext) {
      val cep = iterator.next()
      var map = cep._3
      SendMsgToShence.translate(map)
      SendMsgToShence.writeLog(sa,map)
    }
    sa.flush()
    sa.shutdown()
  }

  /**
    * 分拆事件
    * @param fullAction url
    * @return
    */
  private def getEventValue(fullAction: String): Tuple4[String, String, String, String] = {
    val CEP = "CEP"
    var event_value = ""
    var firstActionName = ""
    var secondActionName = ""
    var lastActionName = ""
    if(StringUtils.isNotEmpty(fullAction)) {
      var uri = fullAction
      val regex = new Regex("""^/(\w|\W)+""")
      if (regex.pattern.matcher(uri).matches()) {
        uri = uri.substring(1)
      }
      val actionArray: Array[String] = uri.split('/')
      actionArray.size match {
        case 1 => {
          event_value = CEP + "_" + actionArray.mkString("_")
        }
        case 2 => {
          event_value = CEP + "_" + actionArray.mkString("_")
        }
        case 3 => {
          if (actionArray(0).equalsIgnoreCase("a")) {
            event_value = CEP + "_" + actionArray.mkString("_")
          } else {
            event_value = CEP + "_" + actionArray(0) + "_" + actionArray(1)
            lastActionName = actionArray(2)
          }
        }
        case 4 => {
          if (actionArray(0).equalsIgnoreCase("a")) {
            event_value = CEP + "_" + actionArray(0) + "_" + actionArray(1) + "_" + actionArray(2)
            lastActionName = actionArray(3)
          } else {
            event_value = CEP + "_" + actionArray(0) + "_" + actionArray(1)
            firstActionName = event_value + "_" + actionArray(2)
            lastActionName = actionArray(3)
          }
        }
        case _ => {
          event_value = CEP + "_" + actionArray(0) + "_" + actionArray(1)
          firstActionName = event_value + "_" + actionArray(2)
          secondActionName = firstActionName + "_" + actionArray(3)
          lastActionName = actionArray(actionArray.size - 1)
        }
      }
    }
    (event_value, firstActionName, secondActionName, lastActionName)
  }

}
