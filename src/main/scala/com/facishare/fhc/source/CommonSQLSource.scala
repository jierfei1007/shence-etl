package com.facishare.fhc.source

import java.util
import java.util.{Date, Map => JMap}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types._

/**
  * Created by jief on 2017/3/3.
  */
object CommonSQLSource {
  /**
    * 创建导入神测的rdd
    * @param hiveContext
    * @param sql
    * @param eventName
    * @param distinctIDName
    * @return
    */
     def createRecordTuple(hiveContext: HiveContext,sql:String,eventName:String,distinctIDName:String)
     :RDD[Tuple3[String, String, JMap[String, Object]]]={
       val commonDF=hiveContext.sql(sql)
       val structType=commonDF.schema
       val fieldsArray=structType.fields
       val isContains=fieldsArray.map(field=>{field.name}).contains(distinctIDName)
       if(!isContains){
         throw new RuntimeException("sql fields does not contains distinctIDName:'"+distinctIDName+"'")
       }
       val commonRDD:RDD[Tuple3[String, String, JMap[String, Object]]]=commonDF.map(row=>{
         val map = new util.HashMap[String, Object]()
         var distinctID="-10000"
         for(field <- fieldsArray){
           var value:Object=null
           field.dataType match {
             case StringType=>{
               value=row.getAs[String](field.name)
             }
             case IntegerType | LongType | DoubleType | FloatType | ShortType =>{
               value=row.getAs(field.name).asInstanceOf[AnyRef]
             }
             case TimestampType =>{
               val time=row.getAs[java.sql.Timestamp](field.name).getTime
               value = new Date(time)
             }
             case DateType=>{
               value=row.getAs[java.util.Date](field.name)
             }
             case BooleanType=>{
               value=row.getAs[Boolean](field.name).asInstanceOf[AnyRef]
             }
             case _ =>{
               value=row.getAs(field.name)
               if(value==null){
                 value=""
               }else{
                 value=value.toString
               }
             }
           }
           if(field.name.equalsIgnoreCase(distinctIDName)){
             distinctID=value.toString
           }
           map.put(field.name,value)
         }
         (distinctID,eventName,map)
       })
       commonRDD
     }
}
