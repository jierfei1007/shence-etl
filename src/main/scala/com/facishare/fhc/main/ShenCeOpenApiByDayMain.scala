package com.facishare.fhc.main

import java.text.SimpleDateFormat
import java.util
import java.util.{Date, Map => JMap}

import com.facishare.fhc.source.OpenApiSource
import com.facishare.fhc.util.{HDFSLogFactory, HDFSUtil, SendMsgToShence}
import com.facishare.fs.cloud.helper.util.ParaJudge
import com.sensorsdata.analytics.javasdk.SensorsAnalytics
import org.apache.commons.lang.StringUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by jief on 2016/12/30.
  */
object ShenCeOpenApiByDayMain {

  def main(args: Array[String]): Unit = {
    /**
      * 判断传入参数个数
      */
    val waringMsg = "XXX.jar \n" +
      " runMode(local,yarn-cluster) \n" +
      " 20161220\n"

    val isExit = !ParaJudge.judge(args, 2, waringMsg)
    val df: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    //如果参数个数错误,则直接退出
    isExit match {
      case true => return
      case _ =>
    }
    val runModel = args(0)
    val dt = args(1)
    //验证参数
    assert(StringUtils.isNotBlank(runModel), "runMode(local,yarn-cluster) can not be blank")
    assert(StringUtils.isNotBlank(dt), "date can not be blank")
    /**
      * 从配置中心获取参数
      */
    //val propConfig = Config.getConfig(configName, runEnvironment)
    //配置spark conf
    val sparkConf = new SparkConf().setAppName("shence-etl-openapi").setMaster(runModel)
    val sparkContext = new SparkContext(sparkConf)
    val accumulator = sparkContext.accumulator(0, "add-shence-nums")
    val hiveContext: HiveContext = new HiveContext(sparkContext)

    //创建api
    val openApiDF = OpenApiSource.getOpenAPIDFbyDay(hiveContext, dt)
    val openapirdd: RDD[Tuple3[String, String, JMap[String, Object]]] = openApiDF.map(row => {
      val map = new util.HashMap[String, Object]()
      val eid = row.getInt(0)
      val elapse = row.getString(1)
      val enterprise_account = row.getString(2)
      val app_id = row.getString(3)
      val error_code = row.getInt(4)
      val interface = row.getString(5)
      val action = row.getString(6)
      val _time = row.getTimestamp(7)

      map.put("openapi_enterprise_id", eid.asInstanceOf[AnyRef])
      map.put("openapi_elapse", elapse)
      map.put("openapi_enterprise_account", enterprise_account)
      map.put("openapi_app_id", app_id)
      map.put("openapi_error_code", error_code.asInstanceOf[AnyRef])
      map.put("openapi_interface", interface)
      map.put("openapi_action", "b_openapi_action")
      map.put("$time", new Date(_time.getTime))
      accumulator.add(1)
      (eid.toString, "b_openapi_action", map)
    })
    //save to shence
    openapirdd.foreachPartition(itor => sendLogToShence(dt)(itor))
    sparkContext.stop()
  }

  /**
    * 发送数据到神测服务
    *
    * @param iterator
    */
  def sendLogToShence(dt: String)(iterator: Iterator[Tuple3[String, String, JMap[String, Object]]]): Unit = {
    val openapi_shence_error_byday_dir: String = com.facishare.fhc.util.Context.shence_error_log_dir + "/" + "cep_shence_openapi_byday/" + dt
    val openapi_shece_error_byday_file: String = openapi_shence_error_byday_dir + "/cep_shence_openapi_byday_" + System.currentTimeMillis() + ".err"
    val hlog = HDFSLogFactory.getHDFSLog(openapi_shece_error_byday_file)
    val sa: SensorsAnalytics = SendMsgToShence.getSA("default")
    while (iterator.hasNext) {
      val cep = iterator.next()
      val map = cep._3
      try {
        SendMsgToShence.writeLog(sa, cep._1, cep._2, map)
      } catch {
        case error: Exception => {
         val outputStream= hlog.getOutPutStream()
          HDFSUtil.write2File(outputStream, error.getMessage)
        }
      }
    }
    sa.flush()
    sa.shutdown()
    hlog.close()
  }
}
