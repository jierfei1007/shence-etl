package com.facishare.fhc.source

import java.util

import com.facishare.fs.cloud.helper.log.PrintLog
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import java.util.{Date, Map => JMap}

/**
  * Created by jief on 2017/1/4.
  */
object QiXinSource {
  /**
    * 企信create session 和 markread session
    * @param hiveContext
    * @param tableName
    * @param dt
    * @return
    */
  def getQXCreateSessionDF(hiveContext: HiveContext, tableName: String, dt: String): DataFrame = {
    val sql =
      """SELECT
                    case when static.enterprise_id is null then -10000 else static.enterprise_id end as enterprise_id,
                    bqc.enterprise_account as enterprise_account,
                    bqc.platform as platform,
                    bqc.employee_id as employee_id,
                    bqc.service_type as service_type,
                    bqc.action as action,
                    bqc.inner_pro_version as inner_pro_version,
                    bqc.message_id as message_id,
                    bqc.session_id as session_id,
                    bqc.message_source_type as message_source_type,
                    bqc.session_type as session_type,
                    bqc.create_session_time as create_session_time
              FROM
              (SELECT
                case when enterprise_account is null or enterprise_account='' then '_default_' else enterprise_account end as enterprise_account,
                platform,
                employee_id,
                service_type,
                action,
                inner_pro_version,
                message_id,
                session_id,
                message_source_type,
                session_type,
                create_session_time
                FROM dw_bds_b.%s where dt='%s')as bqc
              left join
              (SELECT distinct enterprise_id,enterprise_account
                FROM dw_dim.dim_pub_enterprise_info_static WHERE  sk_begin_date <= '%s' AND sk_end_date >= '%s' and run_status =2) as static
              on bqc.enterprise_account=static.enterprise_account"""

    val sql_format = sql.format(tableName, dt, dt, dt)
    PrintLog.log(sql_format)
    hiveContext.sql(sql_format)
  }

  /**
    * create 企信create session 和 markread session RDD[Tuple3[String, String, JMap[String, Object]]]
    * @param qxCreateSessionDF
    * @param eventName
    * @return
    */
  def getQXCreateSessionEventDF(qxCreateSessionDF: DataFrame, eventName: String): RDD[Tuple3[String, String, JMap[String, Object]]] = {
    val qxCreateSessionEventrdd: RDD[Tuple3[String, String, JMap[String, Object]]] = qxCreateSessionDF.map(row => {
      val map = new util.HashMap[String, Object]()
      val enterprise_id = row.getInt(0)
      val enterprise_account = row.getString(1)
      val platform = row.getInt(2)
      val employee_id = row.getInt(3)
      val service_type = row.getInt(4)
      val action = row.getString(5)
      val inner_pro_version = row.getInt(6)
      val message_id = row.getString(7)
      val session_id = row.getString(8)
      val message_source_type = row.getInt(9)
      val session_type = row.getString(10)
      val create_session_time = row.getTimestamp(11)

      map.put("EnterpriseAccount", enterprise_account)
      map.put("Platform", platform.asInstanceOf[AnyRef])
      map.put("FullUserID", enterprise_id + "_" + employee_id)
      map.put("UserID", employee_id.asInstanceOf[AnyRef])
      map.put("ServiceType", service_type.asInstanceOf[AnyRef])
      map.put("FullAction", action)
      map.put("ProductVersion", inner_pro_version.toString)
      map.put("messageId", message_id)
      map.put("sessionId", session_id)
      map.put("messageSourceType", message_source_type.asInstanceOf[AnyRef])
      map.put("sessionType", session_type)
      map.put("$time", new Date(create_session_time.getTime))
      if("b_qx_session_set_detail".equalsIgnoreCase(eventName)) {
        (enterprise_id.toString, "b_qx_createsession_detail", map)
      }else{
        (enterprise_id.toString, eventName, map)
      }
    })
    qxCreateSessionEventrdd
  }


  /**
    *
    * @param hiveContext hiveContext
    * @param dt          day
    * @return
    */
  def getQXMessageGeneralRDD(hiveContext: HiveContext, dt: String): RDD[Tuple3[String, String, JMap[String, Object]]] = {
    val sql =
      """SELECT
                    case when static.enterprise_id is null then -10000 else static.enterprise_id end as enterprise_id,
                    bqc.enterprise_account as enterprise_account,
                    bqc.platform as platform,
                    bqc.employee_id as employee_id,
                    bqc.service_type as service_type,
                    bqc.action as action,
                    bqc.inner_pro_version as inner_pro_version,
                    bqc.message_id as message_id,
                    bqc.session_id as session_id,
                    bqc.message_source_type as message_source_type,
                    bqc.general_message_time as general_message_time,
                    bqc.message_type as message_type,
                    bqc.p_participant_num as p_participant_num
              FROM
              (SELECT
                case when enterprise_account is null or enterprise_account='' then '_default_' else enterprise_account end as enterprise_account,
                platform,
                employee_id,
                service_type,
                action,
                inner_pro_version,
                message_id,
                session_id,
                message_source_type,
                general_message_time,
                message_type,
                p_participant_num
                FROM dw_bds_b.b_qx_message_general_detail where dt='%s')as bqc
              left join
              (SELECT distinct enterprise_id,enterprise_account
                FROM dw_dim.dim_pub_enterprise_info_static WHERE  sk_begin_date <= '%s' AND sk_end_date >= '%s' and run_status =2) as static
              on bqc.enterprise_account=static.enterprise_account"""

    val sql_format = sql.format(dt, dt, dt)
    PrintLog.log(sql_format)
    val QXMessageGeneralDF = hiveContext.sql(sql_format)
    val QXMessageGeneralRDD: RDD[Tuple3[String, String, JMap[String, Object]]] = QXMessageGeneralDF.map(row => {
      val map = new util.HashMap[String, Object]()
      val enterprise_id = row.getInt(0)
      val enterprise_account = row.getString(1)
      val platform = row.getInt(2)
      val employee_id = row.getInt(3)
      val service_type = row.getInt(4)
      val action = row.getString(5)
      val inner_pro_version = row.getInt(6)
      val message_id = row.getLong(7)
      val session_id = row.getString(8)
      val message_source_type = row.getInt(9)
      val general_message_time = row.getTimestamp(10)
      val message_type = row.getString(11)
      val p_participant_num = row.getInt(12)
      map.put("EnterpriseAccount", enterprise_account)
      map.put("Platform", platform.asInstanceOf[AnyRef])
      map.put("FullUserID", enterprise_id + "_" + employee_id)
      map.put("UserID", employee_id.asInstanceOf[AnyRef])
      map.put("ServiceType", service_type.asInstanceOf[AnyRef])
      map.put("FullAction", action)
      map.put("ProductVersion", inner_pro_version.toString)
      map.put("messageId", message_id.toString)
      map.put("sessionId", session_id)
      map.put("messageSourceType", message_source_type.asInstanceOf[AnyRef])
      map.put("$time", new Date(general_message_time.getTime))
      map.put("messageType", message_type)
      map.put("pParticipantNum", p_participant_num.asInstanceOf[AnyRef])

      (enterprise_id.toString, "b_qx_message_general_detail", map)
    })
    QXMessageGeneralRDD
  }

  /**
    *
    * @param hiveContext hiveContext
    * @param dt          day
    * @return
    */
  def getQXMessageigtRDD(hiveContext: HiveContext, dt: String): RDD[Tuple3[String, String, JMap[String, Object]]] = {
    val sql =
      """SELECT
                case when static.enterprise_id is null then -10000 else static.enterprise_id end as enterprise_id,
                bqc.enterprise_account as enterprise_account,
                bqc.platform as platform,
                bqc.employee_id as employee_id,
                bqc.service_type as service_type,
                bqc.action as action,
                bqc.inner_pro_version as inner_pro_version,
                bqc.message_id as message_id,
                bqc.session_id as session_id,
                bqc.message_source_type as message_source_type,
                bqc.message_type as message_type,
                bqc.igt_message_time as igt_message_time,
                bqc.p_workitem_type as p_workitem_type,
                bqc.p_feed_id as p_feed_id
              FROM
              (SELECT
                case when enterprise_account is null or enterprise_account='' then '_default_' else enterprise_account end as enterprise_account,
                platform,
                employee_id,
                service_type,
                action,
                inner_pro_version,
                message_id,
                session_id,
                message_source_type,
                igt_message_time,
                message_type,
                p_workitem_type,
                p_feed_id
                FROM dw_bds_b.b_qx_message_igt_detail where dt='%s')as bqc
              left join
              (SELECT distinct enterprise_id,enterprise_account
                FROM dw_dim.dim_pub_enterprise_info_static WHERE  sk_begin_date <= '%s' AND sk_end_date >= '%s' and run_status =2) as static
              on bqc.enterprise_account=static.enterprise_account"""

    val sql_format = sql.format(dt, dt, dt)
    PrintLog.log(sql_format)
    val QXMessageigtDF = hiveContext.sql(sql_format)
    val QXMessageigtRDD: RDD[Tuple3[String, String, JMap[String, Object]]] = QXMessageigtDF.map(row => {
      val map = new util.HashMap[String, Object]()
      val enterprise_id = row.getInt(0)
      val enterprise_account = row.getString(1)
      val platform = row.getInt(2)
      val employee_id = row.getInt(3)
      val service_type = row.getInt(4)
      val action = row.getString(5)
      val inner_pro_version = row.getInt(6)
      val message_id = row.getLong(7)
      val session_id = row.getString(8)
      val message_source_type = row.getInt(9)
      val message_type = row.getString(10)
      val igt_message_time = row.getTimestamp(11)
      val p_workitem_type = row.getInt(12)
      val p_feed_id = row.getLong(13)

      map.put("EnterpriseAccount", enterprise_account)
      map.put("Platform", platform.asInstanceOf[AnyRef])
      map.put("FullUserID", enterprise_id + "_" + employee_id)
      map.put("UserID", employee_id.asInstanceOf[AnyRef])
      map.put("ServiceType", service_type.asInstanceOf[AnyRef])
      map.put("FullAction", action)
      map.put("ProductVersion", inner_pro_version.toString)
      map.put("messageId", message_id.toString)
      map.put("sessionId", session_id)
      map.put("messageSourceType", message_source_type.asInstanceOf[AnyRef])
      map.put("messageType", message_type)
      map.put("$time", new Date(igt_message_time.getTime))
      map.put("pWorkItemType", p_workitem_type.asInstanceOf[AnyRef])
      map.put("pFeedId", p_feed_id.asInstanceOf[AnyRef])

      (enterprise_id.toString, "b_qx_message_igt_detail", map)
    })
    QXMessageigtRDD
  }
}
