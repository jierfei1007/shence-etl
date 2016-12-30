package com.facishare.fhc.source

import com.facishare.fs.cloud.helper.log.PrintLog
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by jief on 2016/12/30.
  */
object OpenApiSource {


  def getOpenAPIDF(hiveContext:HiveContext,dt:String,hr:String):DataFrame={
    val sql:String="select "+
      " case when static.enterprise_id is null then -1000 else static.enterprise_id end as enterprise_id,"+
      " case when open.elapse is null or open.elapse='null' then '' else open.elapse end as elapse ,"+
      " case when open.enterprise_account is null or open.enterprise_account='null' then '' else open.enterprise_account end as enterprise_account,"+
      " case when open.app_id is null or open.app_id='null' then '' else open.app_id end as app_id,"+
      " case when open.error_code is null then -10000 else open.error_code end as error_code,"+
      " case when open.interface is null or open.interface='null' then '' else open.interface end as interface,"+
      " case when open.action is null or open.action='null' then '' else open.action end as action,"+
      " case when open.`_time` is null then cast('1970-01-01 00:00:00.000' as timestamp) else open.`_time` end as time"+
      " FROM"+
      " (SELECT  elapse, " +
      "          case when enterprise_account is null or enterprise_account='' then '_default_' else enterprise_account end as enterprise_account," +
      "          app_id," +
      "          error_code," +
      "          interface," +
      "          action, " +
      "          `_time` " +
      "  FROM  dw_bds_b.b_openapi_click where dt='"+dt+"' and hr='"+hr+"' and enterprise_account<>'')as open"+
      "  left join"+
      " (SELECT distinct enterprise_id,enterprise_account"+
      "  FROM dw_dim.dim_pub_enterprise_info_static WHERE  sk_begin_date <= '"+dt+"' AND sk_end_date >= '"+dt+"' and run_status =2) as static"+
      "  on"+
      "  open.enterprise_account=static.enterprise_account"
      PrintLog.log("query table b_openapi_click sql:"+sql)
    hiveContext.sql(sql)
  }
}
