package com.facishare.fhc.source

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import java.util.{Date, Locale, Map => JMap}

import org.apache.commons.lang.StringUtils
/**
  * Created by jief on 2017/5/26.
  */
object UserToShenceSource {

  def getUserInfoDF(hiveContext: HiveContext,runDate:String):RDD[(String,JMap[String,Object])]={
    var really_run_date="20170422"
    if(runDate > "20170422"){
      really_run_date=runDate
    }
    val sql=
      """
        select
        A.Enterprise_id as Enterprise_id,
        A.enterprise_account as enterprise_account,
        A.account_total_amount as account_total_amount,
        A.company_scale_desc as company_scale_desc,
        A.enterprise_city as enterprise_city,
        A.enterprise_group_desc as enterprise_group_desc,
        A.enterprise_province_desc as enterprise_province_desc,
        A.enterprise_source_desc as enterprise_source_desc,
        A.enterprise_type_desc as enterprise_type_desc,
        A.industry1_desc as industry1_desc,
        A.industry2_desc as industry2_desc,
        A.industry3_desc as industry3_desc,
        A.registe_time as reg_time,
        A.run_status_desc as run_status_desc,
        A.vendor_id as vendor_id,
        cast(A.app_start_time as timestamp) as app_start_time,
        F.seller_account_dept_name as dept_name,
        case when G.enterprise_id is null then 'N' else 'Y' end as is_gray,
        case when H.enable_account_num is null then 0 else H.enable_account_num end as enable_account_num
        from
        (select * from dw_dim.dim_pub_enterprise_info_static where sk_end_date='29990101')A
        left join
        ( select distinct enterprise_id,seller_account_dept_name from dw_bds_m.m_pub_enterprise_partner where dt='%s' and seller_account_dept_name <> ''
        ) F  on A.Enterprise_id=F.enterprise_id
        left join
        dw_bds_m.m_gray_test_enterprise G  on A.Enterprise_id=G.enterprise_id
        left join
        (select enterprise_id,enable_account_num  from  dw_bds_m.m_pub_enterprise_account_num where dt='%s') H
        on A.Enterprise_id=H.enterprise_id
      """
      val df=hiveContext.sql(sql.format(really_run_date,really_run_date))
      df.printSchema()
      val rdd:RDD[(String,JMap[String,Object])]=df.map(row=>{
        val map = new util.HashMap[String, Object]()
        val Enterprise_id= row.getAs[Int]("Enterprise_id").asInstanceOf[AnyRef]
        val enterprise_account= row.getAs[String]("enterprise_account")
        if(StringUtils.isNotEmpty(enterprise_account)) {
          map.put("enterprise_account",enterprise_account)
        }
        val account_total_amount= row.getAs[Int]("account_total_amount").asInstanceOf[AnyRef]
        if(account_total_amount!=null){
          map.put("account_total_amount",account_total_amount)
        }
        val company_scale_desc = row.getAs[String]("company_scale_desc")
        if(company_scale_desc!=null){
          map.put("company_scale_desc",company_scale_desc)
        }
        val enterprise_city = row.getAs[String]("enterprise_city")
        if(enterprise_city!=null){
          map.put("enterprise_city",enterprise_city)
        }
        val enterprise_group_desc= row.getAs[String]("enterprise_group_desc")
        if(enterprise_group_desc!=null){
          map.put("enterprise_group_desc",enterprise_group_desc)
        }
        val enterprise_province_desc = row.getAs[String]("enterprise_province_desc")
        if(enterprise_province_desc!=null){
          map.put("enterprise_province_desc",enterprise_province_desc)
        }
        val enterprise_source_desc = row.getAs[String]("enterprise_source_desc")
        if(enterprise_source_desc!=null){
          map.put("enterprise_source_desc",enterprise_source_desc)
        }
        val enterprise_type_desc= row.getAs[String]("enterprise_type_desc")
        if(enterprise_type_desc!=null){
          map.put("enterprise_type_desc",enterprise_type_desc)
        }
        val industry1_desc = row.getAs[String]("industry1_desc")
        if(industry1_desc!=null){
          map.put("industry1_desc",industry1_desc)
        }
        val industry2_desc = row.getAs[String]("industry2_desc")
        if(industry2_desc!=null){
          map.put("industry2_desc",industry2_desc)
        }
        val industry3_desc = row.getAs[String]("industry3_desc")
        if(industry3_desc!=null){
          map.put("industry3_desc",industry3_desc)
        }
        val registe_time = row.getAs[String]("reg_time")
        if(StringUtils.isNotBlank(registe_time)){
          val format=new SimpleDateFormat("EEE MMM dd HH:mm:ss zzz yyyy",Locale.US)
          try{
            map.put("reg_time",new Date(format.parse(registe_time).getTime))
          }catch{
            case e:Throwable=>{

            }
          }
        }
        val run_status_desc = row.getAs[String]("run_status_desc")
        if(null!=run_status_desc){
          map.put("run_status_desc",run_status_desc)
        }
        val vendor_id = row.getAs[Int]("vendor_id").asInstanceOf[AnyRef]
        if(vendor_id!=null){
          map.put("vendor_id",vendor_id)
        }
        val app_start_time = row.getAs[Timestamp]("app_start_time")
        if(app_start_time!=null){
            map.put("app_start_time",new Date(app_start_time.getTime))
        }
        val dept_name = row.getAs[String]("dept_name")
        if(dept_name!=null){
          map.put("dept_name",dept_name)
        }
        val is_gray = row.getAs[String]("is_gray")
        map.put("is_gray",is_gray)

        if(!row.isNullAt(18)){
          map.put("enable_account_num",row.getInt(18).asInstanceOf[AnyRef])
        }
        (Enterprise_id.toString,map)
      })
      rdd
  }

  /**
    * 增加了维度
    * @param hiveContext
    * @return
    */
  def getNewUserInfoDF(hiveContext: HiveContext):RDD[(String,JMap[String,Object])]={
    val sql="select * from dw_app_ent_profile.app_ent_profile_sas_daily"
    val df=hiveContext.sql(sql)
    val rdd:RDD[(String,JMap[String,Object])]=df.map(row=>{
      val map = new util.HashMap[String, Object]()
      val Enterprise_id= row.getAs[Int]("enterprise_id").asInstanceOf[AnyRef]
      val enterprise_account= row.getAs[String]("enterprise_account")
      if(StringUtils.isNotEmpty(enterprise_account)) {
        map.put("enterprise_account",enterprise_account)
      }
      val account_total_amount= row.getAs[Int]("account_total_amount").asInstanceOf[AnyRef]
      if(account_total_amount!=null){
        map.put("account_total_amount",account_total_amount)
      }
      val company_scale_desc = row.getAs[String]("company_scale_desc")
      if(company_scale_desc!=null){
        map.put("company_scale_desc",company_scale_desc)
      }
      val enterprise_city = row.getAs[String]("enterprise_city")
      if(enterprise_city!=null){
        map.put("enterprise_city",enterprise_city)
      }
      val enterprise_group_desc= row.getAs[String]("enterprise_group_desc")
      if(enterprise_group_desc!=null){
        map.put("enterprise_group_desc",enterprise_group_desc)
      }
      val enterprise_province_desc = row.getAs[String]("enterprise_province_desc")
      if(enterprise_province_desc!=null){
        map.put("enterprise_province_desc",enterprise_province_desc)
      }
      val enterprise_source_desc = row.getAs[String]("enterprise_source_desc")
      if(enterprise_source_desc!=null){
        map.put("enterprise_source_desc",enterprise_source_desc)
      }
      val enterprise_type_desc= row.getAs[String]("enterprise_type_desc")
      if(enterprise_type_desc!=null){
        map.put("enterprise_type_desc",enterprise_type_desc)
      }
      val industry1_desc = row.getAs[String]("industry1_desc")
      if(industry1_desc!=null){
        map.put("industry1_desc",industry1_desc)
      }
      val industry2_desc = row.getAs[String]("industry2_desc")
      if(industry2_desc!=null){
        map.put("industry2_desc",industry2_desc)
      }
      val industry3_desc = row.getAs[String]("industry3_desc")
      if(industry3_desc!=null){
        map.put("industry3_desc",industry3_desc)
      }
      val registe_time = row.getAs[String]("reg_time")
      if(StringUtils.isNotBlank(registe_time)){
        val format=new SimpleDateFormat("EEE MMM dd HH:mm:ss zzz yyyy",Locale.US)
        try{
          map.put("reg_time",new Date(format.parse(registe_time).getTime))
        }catch{
          case e:Throwable=>{
            println("parse reg_time error"+e.getMessage)
          }
        }
      }
      val run_status_desc = row.getAs[String]("run_status_desc")
      if(null!=run_status_desc){
        map.put("run_status_desc",run_status_desc)
      }
      val vendor_id = row.getAs[Int]("vendor_id").asInstanceOf[AnyRef]
      if(vendor_id!=null){
        map.put("vendor_id",vendor_id)
      }
      val app_start_time = row.getAs[Timestamp]("app_start_time")
      if(app_start_time!=null){
        map.put("app_start_time",new Date(app_start_time.getTime))
      }
      val dept_name = row.getAs[String]("dept_name")
      if(dept_name!=null){
        map.put("dept_name",dept_name)
      }
      val is_gray = row.getAs[String]("is_gray")
      map.put("is_gray",is_gray)

      if(!row.isNullAt(18)){
        map.put("enable_account_num",row.getInt(18).asInstanceOf[AnyRef])
      }
      if(!row.isNullAt(19)){
        map.put("crm_quota_num",row.getInt(19).asInstanceOf[AnyRef])
      }
      if(!row.isNullAt(20)){
        map.put("crm_open_num",row.getInt(20).asInstanceOf[AnyRef])
      }
      if(!row.isNullAt(21)){
        map.put("crm_active_daily",row.getString(21))
      }
      if(!row.isNullAt(22)){
        map.put("crm_active_weekly",row.getString(22))
      }
      if(!row.isNullAt(23)){
        map.put("pau_active_daily",row.getString(23))
      }
      if(!row.isNullAt(24)){
        map.put("huiju_order_prd",row.getString(24))
      }
      if(!row.isNullAt(25)){
        map.put("crm_industry_desc",row.getString(25))
      }
      if(!row.isNullAt(26)){
        map.put("crm_active_monthly",row.getString(26))
      }
      //周流失
      if(!row.isNullAt(27)){
        map.put("crm_active_lost_weekly",row.getString(27))
      }
      //月流失
      if(!row.isNullAt(28)){
        map.put("crm_active_lost_monthly",row.getString(28))
      }
      (Enterprise_id.toString,map)
    })
    rdd
  }

}
