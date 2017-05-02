package com.facishare.fhc.main

import java.net.InetAddress
import java.text.SimpleDateFormat
import java.util
import java.util.{Date, Map => JMap}

import com.facishare.fhc.bean.ShenceCEPServerAction
import com.facishare.fhc.source.CEPServerActionSource
import com.facishare.fhc.util.{HDFSLogFactory, HDFSUtil, JsonUtil, SendMsgToShence}
import com.facishare.fs.cloud.helper.msg.MessageSender
import com.facishare.fs.cloud.helper.util.ParaJudge
import com.sensorsdata.analytics.javasdk.SensorsAnalytics
import org.apache.commons.lang.StringUtils
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{Accumulator, SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer
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
    val df: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
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
    val accumulator:Accumulator[Long] = sparkContext.accumulator(0, "add-shence-nums")
    val errorNums:Accumulator[Long] = sparkContext.accumulator(0, "error-nums")
    val hiveContext: HiveContext = new HiveContext(sparkContext)

    //获取cep server action DataFrame
    val cepDF = CEPServerActionSource.getCEPServerActionDF(hiveContext, dt, hr)
    val cepServerActionBean: RDD[Tuple3[String, String, JMap[String, Object]]] = cepDF.flatMap(row => {
      val cep_array= ArrayBuffer[Tuple3[String, String, JMap[String, Object]]]()
      val map1 = new util.HashMap[String, Object]()
      val map2 = new util.HashMap[String, Object]()
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
      val browser = row.getString(13)
      val version_name = row.getString(14)
      val actions_tuple = getEventValue(action)
      //特殊字符用ascii码16进制符号替换
      var action_value: String = actions_tuple._1
      if (action_value.contains(".")) {
        action_value = action_value.replaceAll("\\.", "_2E_")
      }
      if (action_value.contains("-")) {
        action_value = action_value.replaceAll("-", "_2D_")
      }
      if (action_value.contains("?")) {
        action_value = action_value.replaceAll("\\?", "_3F_")
      }
      if (action_value.contains("&")) {
        action_value = action_value.replaceAll("\\&", "_26_")
      }
      if (action_value.contains("=")) {
        action_value = action_value.replaceAll("=", "_3D_")
      }
      if (action_value.contains("!")) {
        action_value = action_value.replaceAll("\\!", "_21_")
      }
      if (action_value.contains("#")) {
        action_value = action_value.replaceAll("#", "_23_")
      }
      if (action_value.contains(";")) {
        action_value = action_value.replaceAll(";", "_3B_")
      }
      if(StringUtils.isEmpty(action_value)){
        action_value="CEP_"
      }
      map1.put("EventValue", action_value)
      map1.put("Platform", platform.asInstanceOf[AnyRef])
      map1.put("DeviceID", device_id)
      map1.put("IP", employee_ip)
      map1.put("$time", new Date(visit_time.getTime))
      map1.put("Duration", duration.asInstanceOf[AnyRef])
      map1.put("ProductVersion", inner_pro_version)
      map1.put("EnterpriseID", eid.asInstanceOf[AnyRef])
      map1.put("UserID", employee_id.asInstanceOf[AnyRef])
      map1.put("ServiceType", service_type.asInstanceOf[AnyRef])
      map1.put("OSVersion", os_version)
      map1.put("BrowserVersion", browser_version)
      map1.put("Browser", browser)
      map1.put("FullAction", action)
      map1.put("FirstActionName", actions_tuple._2)
      map1.put("SecondActionName", actions_tuple._3)
      map1.put("LastActionName", actions_tuple._4)
      map1.put("VersionName", version_name)
      map1.put("FullUserID", eid.toString + "_" + employee_id.toString)

      cep_array+=((eid.toString, action_value, map1))

      map2.put("EventValue", "CEP")
      map2.put("Platform", platform.asInstanceOf[AnyRef])
      map2.put("DeviceID", device_id)
      map2.put("IP", employee_ip)
      map2.put("$time", new Date(visit_time.getTime))
      map2.put("Duration", duration.asInstanceOf[AnyRef])
      map2.put("ProductVersion", inner_pro_version)
      map2.put("EnterpriseID", eid.asInstanceOf[AnyRef])
      map2.put("UserID", employee_id.asInstanceOf[AnyRef])
      map2.put("ServiceType", service_type.asInstanceOf[AnyRef])
      map2.put("OSVersion", os_version)
      map2.put("BrowserVersion", browser_version)
      map2.put("Browser", browser)
      map2.put("FullAction", action)
      map2.put("FirstActionName", actions_tuple._2)
      map2.put("SecondActionName", actions_tuple._3)
      map2.put("LastActionName", actions_tuple._4)
      map2.put("VersionName", version_name)
      map2.put("FullUserID", eid.toString + "_" + employee_id.toString)
      cep_array+=((eid.toString,"CEP", map2))
      cep_array
    })
    //save to shence
    println("partition num is="+cepServerActionBean.getNumPartitions)
    cepServerActionBean.coalesce(20,true).foreachPartition(itor => sendLogToShence(accumulator,errorNums,dt, hr)(itor))

    val nums=errorNums.value
    if(nums>0){
      val msg="[仓库数据入神测] \ncep to shence by hour error numbers is:"+nums+"\n"+"dt:"+dt+",hr:"+hr+"\n [负责人: 田春;魏磊;王杰朝;武靖;纪二飞;王正坤;王海利;姚致远][发送人：武靖]"
      MessageSender.sendMsg(msg,Array(4998,4097,3719,6021,1368,4686,5458))
    }
    sparkContext.stop()
  }

  /**
    * 发送数据到神测服务
    *
    * @param iterator
    */
  def sendLogToShence(accumulator: Accumulator[Long],errorNums:Accumulator[Long],dt: String, hr: String)(iterator: Iterator[Tuple3[String, String, JMap[String, Object]]]): Unit = {
    val cep_shence_error_byhour_dir: String = com.facishare.fhc.util.Context.shence_error_log_dir + "/" + "cep_server_action_shence_byhour/" + dt + "/" + hr
    val cep_shece_error_byhour_file: String = cep_shence_error_byhour_dir + "/cep_server_action_shence_byhour_" + System.currentTimeMillis() + ".err"
    val hlog = HDFSLogFactory.getHDFSLog(cep_shece_error_byhour_file)
    SendMsgToShence.setProvInfo()
    val sa: SensorsAnalytics =SendMsgToShence.getSA("default")
    while (iterator.hasNext) {
      val cep = iterator.next()
      var map = cep._3
      SendMsgToShence.translate(map)
      try {
        SendMsgToShence.writeLog(sa, cep._1, cep._2, map)
        accumulator.add(1L)
      } catch {
        case error: Exception => {
          errorNums.add(1L)
          val outputStream = hlog.getOutPutStream()
          HDFSUtil.write2File(outputStream, error.getMessage)
        }
      }
    }
    sa.flush()
    sa.shutdown()
    hlog.close()
  }

  /**
    * 分拆事件
    *
    * @param fullAction url
    * @return
    */
  private def getEventValue(fullAction: String): Tuple4[String, String, String, String] = {
    val CEP = "CEP"
    var event_value = ""
    var firstActionName = ""
    var secondActionName = ""
    var lastActionName = ""
    if (StringUtils.isNotEmpty(fullAction)) {
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
