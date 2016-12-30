package com.facishare.fhc.source

import com.facishare.fs.cloud.helper.log.PrintLog
import com.facishare.fs.cloud.spark.context.HuijuContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by jief on 2016/12/20.
  */
object CEPServerActionSource {


  def getCEPServerActionDF(hiveContext:HiveContext,dt:String,hr:String):DataFrame={
    val sql="select " +
      " `action`,"+
      "  case when platform is null then -10000 else platform end as platform,"+
      "  case when device_id is null or device_id='null' then '' else device_id end as device_id ,"+
      "  case when employee_ip is null or employee_ip='null' then '127.0.0.1' else employee_ip end as employee_ip,"+
      "  case when visit_time is null then cast('1970-01-01 00:00:00.000' as timestamp) else visit_time end as visit_time,"+
      "  case when duration is null then -10000 else duration end as duration ,"+
      "  case when inner_pro_version is null or inner_pro_version='null' then '' else inner_pro_version end as inner_pro_version,"+
      "  case when eid is null then -10000 else eid end as eid ,"+
      "  case when employee_id is null then -10000 else employee_id end as employee_id ,"+
      "  case when service_type is null then -10000 else service_type end as service_type ,"+
      "  case when os_version is null or os_version='null' then '' else os_version end as os_version,"+
      "  case when browser_version is null or browser_version='null' then '' else browser_version end as browser_version,"+
      "  case when `_time` is null then cast('1970-01-01 00:00:00.000' as timestamp) else `_time` end as time,"+
      "  case when browser is null or browser='null' then '' else browser end as browser,"+
      "  case when version_name is null or version_name='null' then '' else version_name end as version_name"+
      " from dw_bds_b.b_cep_server_action_detail where dt='"+dt+"' and hr='"+hr+"'"
    PrintLog.log("query table dw_bds_b.b_cep_server_action_detail:"+sql)
    hiveContext.sql(sql)
  }

}
