package com.facishare.fhc.source.xt

import java.util
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import java.util.{Date, Map => JMap}
import com.facishare.fhc.util.HbaseCommonUtil
import org.apache.hadoop.hbase.client.{Get, Table}
import org.apache.hadoop.hbase.util.Bytes
import scala.collection.mutable.ListBuffer
/**
  * Created by jief on 2017/6/22.
  */
object XTFeedSource {
   /**
    * feed reply to 神策
    * @param hiveContext
    * @param runDate
    * @param propConfig
    * @return
    */
   def createFeedReplyDF(hiveContext: HiveContext,runDate:String,propConfig: java.util.Map[String, String]):RDD[(String, String, JMap[String, Object])]={
     val sql=
       s"""
         |select
         |eid as EnterpriseID,
         |employee_id as UserID,
         |concat(eid,'-',employee_id) as FullUserID,
         |platform as Platform,
         |action as action,
         |sendfeed_reply_time as time,
         |duration as Duration,
         |service_type as ServiceType,
         |p_feed_reply_id,
         |p_isat_employee,
         |p_isat_circle,
         |p_source,
         |p_feed_id,
         |case when p_feed_create_time<'1970-01-01' then cast('1970-01-01 00:00:00.000' as timestamp) else p_feed_create_time end as p_feed_create_time,
         |p_feed_creator_id,
         |p_approve_employee_id,
         |case when  p_approve_time<'1970-01-01' then cast('1970-01-01 00:00:00.000' as timestamp) else p_approve_time end as p_approve_time,
         |p_approve_minutes,
         |p_xt_sendfeedreply_numeric_a,
         |p_xt_sendfeedreply_numeric_b,
         |p_xt_sendfeedreply_numeric_c
         |from dw_bds_b.b_xt_sendfeedreply_detail where dt='$runDate'
       """.stripMargin
      val df=hiveContext.sql(sql)
      val rdd:RDD[(String, String, JMap[String, Object])]=df.mapPartitions(row_itor=>{
        val conn_table=HbaseCommonUtil.getExistHbaseTable(propConfig,"SENDFEED_TYPE")
        val table:Table=conn_table._2
        val list=ListBuffer[(String, String, JMap[String, Object])]()
        while(row_itor.hasNext){
          val map = new util.HashMap[String, Object]()
          val row=row_itor.next()
          var EnterpriseID:Int = -10000
          if(!row.isNullAt(0)){
            EnterpriseID=row.getInt(0)
          }
          map.put("EnterpriseID",EnterpriseID.asInstanceOf[Integer])
          if(!row.isNullAt(1)){
            val UserID=row.getInt(1)
            map.put("UserID",UserID.asInstanceOf[Integer])
          }
          map.put("FullUserID",row.getString(2))
          if(!row.isNullAt(3)){
            map.put("Platform",row.getInt(3).asInstanceOf[AnyRef])
          }
          map.put("action",row.getString(4))
          if(!row.isNullAt(5)) {
            map.put("$time",new Date(row.getTimestamp(5).getTime))
          }
          if(!row.isNullAt(6)){
            map.put("Duration",row.getInt(6).asInstanceOf[AnyRef])
          }
          if(!row.isNullAt(7)){
            map.put("ServiceType",row.getInt(7).asInstanceOf[AnyRef])
          }
          if(!row.isNullAt(8)){
            map.put("p_feed_reply_id",row.getInt(8).asInstanceOf[AnyRef])
          }
          if(!row.isNullAt(9)){
            map.put("p_isat_employee",row.getBoolean(9).asInstanceOf[AnyRef])
          }
          if(!row.isNullAt(10)){
            map.put("p_isat_circle",row.getBoolean(10).asInstanceOf[AnyRef])
          }
          map.put("p_source",row.getString(11))
          var p_feed_id:Long = -10000
          if(!row.isNullAt(12)){
            p_feed_id=row.getLong(12)
          }
          map.put("p_feed_id",p_feed_id.asInstanceOf[AnyRef])
          if(!row.isNullAt(13)){
            map.put("p_feed_create_time",new Date(row.getTimestamp(13).getTime))
          }
          if(!row.isNullAt(14)){
            map.put("p_feed_creator_id",row.getInt(14).asInstanceOf[AnyRef])
          }
          if(!row.isNullAt(15)){
            map.put("p_approve_employee_id",row.getInt(15).asInstanceOf[AnyRef])
          }
          if(!row.isNullAt(16)){
            map.put("p_approve_time",new Date(row.getTimestamp(16).getTime))
          }
          if(!row.isNullAt(17)){
            map.put("p_approve_minutes",row.getInt(17).asInstanceOf[AnyRef])
          }
          if(!row.isNullAt(18)){
            map.put("p_xt_sendfeedreply_numeric_a",row.getInt(18).asInstanceOf[AnyRef])
          }
          if(!row.isNullAt(19)){
            map.put("p_xt_sendfeedreply_numeric_b",row.getInt(19).asInstanceOf[AnyRef])
          }
          if(!row.isNullAt(20)){
            map.put("p_xt_sendfeedreply_numeric_c",row.getInt(20).asInstanceOf[AnyRef])
          }
          getDataFromHbase(EnterpriseID+"-"+p_feed_id,table,map)
//          list+=((EnterpriseID.toString,"b_xt_sendfeedreply_detail_v2",map))
          list+=((EnterpriseID.toString,"b_xt_sendfeedreply_detail",map))
        }
        list.iterator
      })
     rdd
   }

  /**
    * send Receipt to 神策
    * @param hiveContext
    * @param runDate
    * @param propConfig
    * @return
    */
  def createSendReceipt(hiveContext: HiveContext,runDate:String,propConfig: java.util.Map[String, String]):RDD[(String, String, JMap[String, Object])]={
    val sql=
      s"""
        |select
        |eid as EnterpriseID,
        |employee_id as UserID,
        |concat(eid,'-',employee_id) as FullUserID,
        |platform as Platform,
        |action as action,
        |sent_receipt_time as time,
        |duration as Duration,
        |service_type as ServiceType,
        |p_feed_id as p_feed_id
        |from dw_bds_b.b_xt_sentreceipt_detail where dt='$runDate'
      """.stripMargin
    val df=hiveContext.sql(sql)
    val rdd:RDD[(String, String, JMap[String, Object])]=df.mapPartitions(row_itor=>{
      val conn_table=HbaseCommonUtil.getExistHbaseTable(propConfig,"SENDFEED_TYPE")
      val table:Table=conn_table._2
      val list=ListBuffer[(String, String, JMap[String, Object])]()
      while(row_itor.hasNext) {
        val map = new util.HashMap[String, Object]()
        val row = row_itor.next()
        var EnterpriseID = -10000
        if(!row.isNullAt(0)){
          EnterpriseID=row.getInt(0)
        }
        map.put("EnterpriseID",EnterpriseID.asInstanceOf[AnyRef])
        if(!row.isNullAt(1)){
          map.put("UserID",row.getInt(1).asInstanceOf[AnyRef])
        }
        map.put("FullUserID",row.getString(2))
        if(!row.isNullAt(3)){
          map.put("Platform",row.getInt(3).asInstanceOf[AnyRef])
        }
        map.put("action",row.getString(4))
        if(!row.isNullAt(5)){
          map.put("$time",new Date(row.getTimestamp(5).getTime))
        }
        if(!row.isNullAt(6)){
          map.put("Duration",row.getInt(6).asInstanceOf[AnyRef])
        }
        if(!row.isNullAt(7)){
          map.put("ServiceType",row.getInt(7).asInstanceOf[AnyRef])
        }
        var p_feed_id = -10000L
        if(!row.isNullAt(8)){
          p_feed_id = row.getLong(8)
        }
        map.put("p_feed_id",p_feed_id.asInstanceOf[AnyRef])
        getDataFromHbase(EnterpriseID+"-"+p_feed_id,table,map)
        list+=((EnterpriseID.toString,"b_xt_sentreceipt_detail",map))
//        list+=((EnterpriseID.toString,"b_xt_sentreceipt_detail_v2",map))
      }
      list.iterator
    })
    rdd
  }

  /**
    * feed like to神策
    * @param hiveContext
    * @param runDate
    * @param propConfig
    * @return
    */
  def createFeedLike(hiveContext: HiveContext,runDate:String,propConfig: java.util.Map[String, String]):RDD[(String, String, JMap[String, Object])]={
    val sql=
      s"""
        |select
        |eid as EnterpriseID,
        |employee_id as UserID,
        |concat(eid,'-',employee_id) as FullUserID,
        |platform as Platform,
        |action as action,
        |feedlike_time as time,
        |duration as Duration,
        |service_type as ServiceType,
        |p_like_type as p_like_type,
        |p_to_employ_id as p_to_employ_id,
        |p_from_employ_id as p_from_employ_id,
        |p_like_time as p_like_time,
        |p_feed_id as p_feed_id
        |from dw_bds_b.b_xt_feedlike_detail where dt='$runDate'
      """.stripMargin
    val df=hiveContext.sql(sql)
    val rdd:RDD[(String, String, JMap[String, Object])]=df.mapPartitions(row_itor=>{
      val conn_table=HbaseCommonUtil.getExistHbaseTable(propConfig,"SENDFEED_TYPE")
      val table:Table=conn_table._2
      val list=ListBuffer[(String, String, JMap[String, Object])]()
      while(row_itor.hasNext) {
        val map = new util.HashMap[String, Object]()
        val row = row_itor.next()
        var EnterpriseID = -10000
        if(!row.isNullAt(0)){
          EnterpriseID=row.getInt(0)
        }
        map.put("EnterpriseID",EnterpriseID.asInstanceOf[AnyRef])
        if(!row.isNullAt(1)){
          map.put("UserID",row.getInt(1).asInstanceOf[AnyRef])
        }
        map.put("FullUserID",row.getString(2))
        if(!row.isNullAt(3)){
          map.put("Platform",row.getInt(3).asInstanceOf[AnyRef])
        }
        map.put("action",row.getString(4))
        if(!row.isNullAt(5)){
          map.put("$time",new Date(row.getTimestamp(5).getTime))
        }
        if(!row.isNullAt(6)){
          map.put("Duration",row.getInt(6).asInstanceOf[AnyRef])
        }
        if(!row.isNullAt(7)){
          map.put("ServiceType",row.getInt(7).asInstanceOf[AnyRef])
        }
        if(!row.isNullAt(8)){
          map.put("p_like_type",row.getInt(8).asInstanceOf[AnyRef])
        }
        if(!row.isNullAt(9)){
          map.put("p_to_employ_id",row.getInt(9).asInstanceOf[AnyRef])
        }
        if(!row.isNullAt(10)){
          map.put("p_from_employ_id",row.getInt(10).asInstanceOf[AnyRef])
        }
        if(!row.isNullAt(11)){
          map.put("p_like_time",new Date(row.getTimestamp(11).getTime))
        }
        var p_feed_id:Long= -10000
        if(!row.isNullAt(12)){
          p_feed_id=row.getLong(12)
        }
        map.put("p_feed_id",p_feed_id.asInstanceOf[AnyRef])
        getDataFromHbase(EnterpriseID+"-"+p_feed_id,table,map)
        list+=((EnterpriseID.toString,"b_xt_feedlike_detail",map))
//        list+=((EnterpriseID.toString,"b_xt_feedlike_detail_v2",map))
      }
      list.iterator
    })
    rdd
  }

  /**
    *
    * @param row_key
    * @param table
    * @param map
    */
  def getDataFromHbase(row_key:String,table:Table,map:JMap[String, Object]):Unit={
    val family_bytes = Bytes.toBytes("types")
    val p_feed_type = "p_feed_type".getBytes
    val p_feed_plan_type = "p_feed_plan_type".getBytes
    val p_feed_approve_type = "p_feed_approve_type".getBytes
    val p_approve_flow_type = "p_approve_flow_type".getBytes
    val p_xt_sendfeed_numeric_a = "p_xt_sendfeed_numeric_a".getBytes
    val p_xt_sendfeed_numeric_b = "p_xt_sendfeed_numeric_b".getBytes
    val p_xt_sendfeed_numeric_c = "p_xt_sendfeed_numeric_c".getBytes
    val get= new Get(Bytes.toBytes(row_key))
    get.addFamily(family_bytes)
    get.addColumn(family_bytes,p_feed_type)
    get.addColumn(family_bytes,p_feed_plan_type)
    get.addColumn(family_bytes,p_feed_approve_type)
    get.addColumn(family_bytes,p_approve_flow_type)
    get.addColumn(family_bytes,p_xt_sendfeed_numeric_a)
    get.addColumn(family_bytes,p_xt_sendfeed_numeric_b)
    get.addColumn(family_bytes,p_xt_sendfeed_numeric_c)
    val result=table.get(get)
    if(!result.isEmpty){
      val cell_p_feed_type = result.getColumnLatestCell(family_bytes,p_feed_type)
      val cell_p_feed_plan_type = result.getColumnLatestCell(family_bytes,p_feed_plan_type)
      val cell_p_feed_approve_type = result.getColumnLatestCell(family_bytes,p_feed_approve_type)
      val cell_p_approve_flow_type = result.getColumnLatestCell(family_bytes,p_approve_flow_type)
      val cell_p_xt_sendfeed_numeric_a = result.getColumnLatestCell(family_bytes,p_xt_sendfeed_numeric_a)
      val cell_p_xt_sendfeed_numeric_b = result.getColumnLatestCell(family_bytes,p_xt_sendfeed_numeric_b)
      val cell_p_xt_sendfeed_numeric_c = result.getColumnLatestCell(family_bytes,p_xt_sendfeed_numeric_c)
      if(null!=cell_p_feed_type){
        map.put("p_feed_type",new Integer(Bytes.toInt(cell_p_feed_type.getValueArray)))
      }
      if(null!=cell_p_feed_plan_type){
        map.put("cell_p_feed_plan_type",new Integer(Bytes.toInt(cell_p_feed_plan_type.getValueArray)))
      }
      if(null!=cell_p_feed_approve_type){
        map.put("p_feed_approve_type",new Integer(Bytes.toInt(cell_p_feed_approve_type.getValueArray)))
      }
      if(null!=cell_p_approve_flow_type){
        map.put("p_approve_flow_type",new Integer(Bytes.toInt(cell_p_approve_flow_type.getValueArray)))
      }
      if(null!=cell_p_xt_sendfeed_numeric_a){
        map.put("p_xt_sendfeed_numeric_a",new Integer(Bytes.toInt(cell_p_xt_sendfeed_numeric_a.getValueArray)))
      }
      if(null!=cell_p_xt_sendfeed_numeric_b){
        map.put("p_xt_sendfeed_numeric_b",new Integer(Bytes.toInt(cell_p_xt_sendfeed_numeric_b.getValueArray)))
      }
      if(null!=cell_p_xt_sendfeed_numeric_c){
        map.put("p_xt_sendfeed_numeric_c",new Integer(Bytes.toInt(cell_p_xt_sendfeed_numeric_c.getValueArray)))
      }
    }
  }

}
