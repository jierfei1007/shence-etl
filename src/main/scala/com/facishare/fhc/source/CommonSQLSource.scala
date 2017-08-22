package com.facishare.fhc.source

import java.util
import java.util.{Date, Map => JMap}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types._

import scala.collection.mutable

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
         for(index <- 0 until fieldsArray.length){
           var value:Object=null
           fieldsArray(index).dataType.typeName match {
             case "string" =>{
               if(!row.isNullAt(index)) {
                 value = row.getString(index)
               }
             }
             case "integer" | "long" | "double" | "float" | "short" =>{
               value=row.getAs(fieldsArray(index).name).asInstanceOf[AnyRef]
             }
             case "decimal"=>{value=row.getAs[Decimal](fieldsArray(index).name).asInstanceOf[AnyRef]}
             case "timestamp" =>{
               val time=row.getAs[java.sql.Timestamp](fieldsArray(index).name)
               if(time!=null){
                 value = new Date(time.getTime)
               }
             }
             case "date" =>{
               if(!row.isNullAt(index)){
                 value=row.getDate(index)
               }
             }
             case "boolean" =>{value=row.getAs[Boolean](fieldsArray(index).name).asInstanceOf[AnyRef]}
             case "array" =>{
               if(!row.isNullAt(index)){
                 value = row.getList(index)
               }
             }
             case _ =>{
               value=row.getAs(fieldsArray(index).name)
               if(value!=null){
                 value=value.toString
               }
             }
           }
           if(fieldsArray(index).name.equalsIgnoreCase(distinctIDName)){
             if(null == value){
               distinctID="-1000"
             }else{
               distinctID = value.toString
             }
           }
           if(value!=null) {
             map.put(fieldsArray(index).name, value)
           }
         }
         (distinctID,eventName,map)
       })
       commonRDD
     }
}
