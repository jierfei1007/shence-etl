package com.facishare.fhc.source

import java.util
import java.util.Map

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import java.util.{Date, Map => JMap}

import org.apache.spark.sql.hive.HiveContext

/**
  * Created by jief on 2017/2/23.
  */
object UserLoginSource {

  /**
    *
    * @param hiveContext
    * @param dt
    * @return
    */
  def getUserLoginDF(hiveContext: HiveContext,dt:String):RDD[Tuple3[String, String, JMap[String, Object]]]={

    val sql:String = "select "+
          " case when client_version is null then '' else client_version end as client_version, "+
          " case when device_id is null then '' else device_id end as device_id,"+
          " case when enterprise_id is null then -10000 else enterprise_id end as enterprise_id, "+
          " case when ip is null then '' else ip end as ip, "+
          " case when os_version is null then '' else os_version end as os_version, "+
          " case when platform is null then -10000 else platform end as platform, "+
          " case when date_version is null then '' else date_version end as date_version, "+
          " case when `time` is null then cast('1970-01-01 00:00:00.000' as timestamp) else `time` end as `time`, "+
          " case when user_id is null then -10000 else user_id end as user_id, "+
          " case when business_code is null then -10000 else business_code end as business_code, "+
          " case when login_type is null then -10000 else login_type end as login_type, "+
          " case when mobile is null then '' else mobile end as mobile, "+
          " case when enterprise_account is null then '' else enterprise_account end as enterprise_account, "+
          " case when account is null then '' else account end as account, "+
          " case when action is null then '' else action  end as action "+
      " from dw_login_b.b_user_login_cgi where dt='"+dt+"' "
    println(sql)
    val dataFrame=hiveContext.sql(sql)
    val rdd:RDD[Tuple3[String, String, JMap[String, Object]]] =dataFrame.map(row=>{
      val map = new util.HashMap[String, Object]()
      val client_version=row.getString(0)
      val device_id=row.getString(1)
      val enterprise_id=row.getInt(2)
      val ip=row.getString(3)
      val os_version=row.getString(4)
      val platform=row.getInt(5)
      val date_version=row.getString(6)
      val time=row.getTimestamp(7)
      val user_id=row.getInt(8)
      val business_code=row.getInt(9)
      val login_type=row.getInt(10)
      val mobile=row.getString(11)
      val enterprise_account=row.getString(12)
      val account=row.getString(13)
      val action=row.getString(14)
      map.put("client_version",client_version)
      map.put("device_id",device_id)
      map.put("enterprise_id",enterprise_id.asInstanceOf[AnyRef])
      map.put("ip",ip)
      map.put("os_version",os_version)
      map.put("platform",platform.asInstanceOf[AnyRef])
      map.put("date_version",date_version)
      map.put("$time",new Date(time.getTime))
      map.put("userId",user_id.asInstanceOf[AnyRef])
      map.put("business_code",business_code.toString)
      map.put("login_type",login_type.asInstanceOf[AnyRef])
      map.put("mobile",mobile)
      map.put("enterprise_account",enterprise_account)
      map.put("account",account)
      map.put("action",action)

      (enterprise_id+"","b_user_login_cgi",map)
    })
    rdd
  }

  /**
    *
    * @param hiveContext
    * @return
    */
   def getUserLoginStatic(hiveContext: HiveContext):RDD[Tuple3[String, String, JMap[String, Object]]]={
     val sql:String=""" select
                         case when business_code is null then '' else business_code end as business_code,
                         case when platform is null then -10000 else platform end as platform,
                         case when client_version is null then '' else client_version end as client_version ,
                         case when os_version is null then '' else os_version end as os_version ,
                         case when total_count is null then -10000 else total_count end as total_count,
                         case when dt is null then '19700101' else dt end as dt
                        from  dw_login_b.b_user_login_statistic """

     println(sql)
     val dataFrame=hiveContext.sql(sql)
     val rdd:RDD[Tuple3[String, String, JMap[String, Object]]]=dataFrame.map(row=>{
       val map = new util.HashMap[String, Object]()
       map.put("business_code",row.getString(0))
       map.put("platform",row.getInt(1).asInstanceOf[AnyRef])
       map.put("client_version",row.getString(2))
       map.put("os_version",row.getString(3))
       map.put("total_count",row.getInt(4).asInstanceOf[AnyRef])
       map.put("dt",row.getString(5))
       (row.getString(5),"b_user_login_statistic",map)
     })
     rdd
   }
}
