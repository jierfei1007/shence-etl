package com.facishare.fhc.main.test

import java.util
import java.util.Date

import com.facishare.fhc.util.{HDFSUtil, SendMsgToShence}
import com.sensorsdata.analytics.javasdk.SensorsAnalytics

/**
  * Created by jief on 2017/2/9.
  */
object TestSendToShence {

  def main(args: Array[String]): Unit = {
    val map = new util.HashMap[String, Object]()
    map.put("EventValue", "CEP_a_iphone_GlobalInfo")
    map.put("Platform", 1203.asInstanceOf[AnyRef])
    map.put("DeviceID", "AF24E3DB-E0EA-46BC-9BAF-CB51D7B24A87")
    map.put("IP", "113.5.2.26")
    map.put("$ip","113.5.2.26")
    map.put("$time", new Date(1476834917000L))
    map.put("Duration", 5.asInstanceOf[AnyRef])
    map.put("ProductVersion", "57")
    map.put("EnterpriseID", 306386.asInstanceOf[AnyRef])
    map.put("UserID", 1007.asInstanceOf[AnyRef])
    map.put("ServiceType", 0.asInstanceOf[AnyRef])
    map.put("OSVersion", "9.3.5")
    map.put("BrowserVersion", "")
    map.put("Browser", "")
    map.put("FullAction", "/a/iphone/GlobalInfo/SubmitPnsTokenForIOS")
    map.put("FirstActionName","")
    map.put("SecondActionName", "")
    map.put("LastActionName", "SubmitPnsTokenForIOS")
    map.put("VersionName", "5.3.1")
    map.put("FullUserID", "306386_1007")
    val sa: SensorsAnalytics = new SensorsAnalytics(new SensorsAnalytics.BatchConsumer("http://sasdata.foneshare.cn/sa?project=default", 10))
    try
      sa.track("306386", "CEP_a_iphone_GlobalInfo", map)
    catch {
      case e: Exception => {
        throw new RuntimeException("writeLog error:" + map + "; errormsg=" + e.getMessage)
      }
    }
    sa.flush()
    sa.shutdown()
  }
}
