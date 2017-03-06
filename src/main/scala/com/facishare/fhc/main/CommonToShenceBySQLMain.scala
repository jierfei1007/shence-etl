package com.facishare.fhc.main

import java.net.InetAddress
import java.util.{Date, Map => JMap}

import com.facishare.fhc.source.CommonSQLSource
import com.facishare.fhc.util.{HDFSLogFactory, HDFSUtil, SendMsgToShence}
import com.facishare.fs.cloud.helper.msg.MessageSender
import com.sensorsdata.analytics.javasdk.SensorsAnalytics
import org.apache.commons.lang.StringUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{Accumulator, SparkConf, SparkContext}

/**
  * Created by jief on 2017/3/3.
  */
object CommonToShenceBySQLMain {


  def main(args: Array[String]): Unit = {
    if(args.length<7){
      throw new RuntimeException("args error !" +
        "spark-submit \n" +
        "--class com.facishare.fhc.main.CommonToShenceBySQLMain \n" +
        "--queue huiju \n"+
        "--executor-memory 4G \n"+
        "--num-executors 20 \n "+
        "--master yarn-cluster \n"+
        "hdfs:///JARS/fhc-shence/*.jar \n"+
        "open-api enterprise_id hdfs:///user/tmp/open-api.sql 20170203,08 default openapitoshence yarn-cluster"
      )
    }
    //事件名称
    val eventName=args(0)
    assert(StringUtils.isNotEmpty(eventName),"event name can not be empty!")
    //用户的原始ID名称
    val distinctIDName=args(1)
    assert(StringUtils.isNotEmpty(distinctIDName),"shence distinct id name can not be empty!")
    //sql文件的hdfs文件路径
    val sqlFilePath=args(2)
    assert(StringUtils.isNotEmpty(sqlFilePath),"sql file path can not be empty!")
    //sql format 用","分隔 最多支持4个参数替换
    val sqlParams=args(3)
    //神策project 名称
    val shenCeProject=args(4)
    assert(StringUtils.isNotEmpty(shenCeProject),"shen ce project name can not be empty!")
    //任务名称
    val taskTitle=args(5)
    assert(StringUtils.isNotEmpty(taskTitle),"task title can not be empty!")
    //运行master
    val master=args(6)
    assert(StringUtils.isNotEmpty(master),"spark master can not be empty")
    //sql 脚本
    var sql=HDFSUtil.readHDFSTextFile(sqlFilePath)
    val params=sqlParams.split(",")
    //支持4个format替换
    if(params.length==1){
      sql=sql.format(params(0))
    }else if(params.length==2){
      sql=sql.format(params(0),params(1))
    }else if(params.length==3){
      sql=sql.format(params(0),params(1),params(2))
    }else if(params.length==4){
      sql=sql.format(params(0),params(1),params(2),params(3))
    }else{
      throw new RuntimeException("sql params split by ',' to  a array's length must leg 4 ")
    }

    val sparkConf = new SparkConf().setAppName(taskTitle).setMaster(master)
    val sparkContext = new SparkContext(sparkConf)
    val accumulator:Accumulator[Long] = sparkContext.accumulator(0, "add-shence-nums")
    val errorNums:Accumulator[Long] = sparkContext.accumulator(0, "error-nums")
    val hiveContext: HiveContext = new HiveContext(sparkContext)
    val commonRDD:RDD[Tuple3[String, String, JMap[String, Object]]]=CommonSQLSource.createRecordTuple(hiveContext,sql,eventName,distinctIDName)
    commonRDD.foreachPartition(itor=>sendLogToShence(accumulator,errorNums,taskTitle,shenCeProject)(itor))
    val nums=errorNums.value
    if(nums>0){
      val msg=taskTitle+"by day error numbers is:"+nums+"\n sql params:"+sqlParams
      MessageSender.sendMsg(msg,Array(4097,3719,6021,1368))
    }
    val oknums=accumulator.value
    if(oknums < 10){
      val msg=taskTitle+"by day add numbers is:"+oknums+"\n sql params:"+sqlParams+" please check!"
      MessageSender.sendMsg(msg,Array(4097,3719,6021,1368))
    }
    sparkContext.stop()
  }
  /**
    * 发送数据到神测服务
    * @param iterator
    */
  def sendLogToShence(accumulator: Accumulator[Long],errorNums:Accumulator[Long],taskTitle:String,projectName:String)(iterator: Iterator[Tuple3[String,String,JMap[String,Object]]]): Unit ={
    //初始化hdfs报错路径
    val cep_error_log_dir:String= com.facishare.fhc.util.Context.shence_error_log_dir+"/"+taskTitle+"/"
    val iAddress: InetAddress = InetAddress.getLocalHost
    val hostName: String = iAddress.getHostName
    val cep_error_log_file:String=cep_error_log_dir+taskTitle+"_"+hostName+"_"+System.currentTimeMillis()+".err"
    val hlog = HDFSLogFactory.getHDFSLog(cep_error_log_file)
    val sa: SensorsAnalytics =SendMsgToShence.getSA(projectName)
    while (iterator.hasNext) {
      val cep = iterator.next()
      var map = cep._3
      SendMsgToShence.translateProperty(map)
      try{
        SendMsgToShence.writeLog(sa,cep._1,cep._2,map)
        accumulator.add(1L)
      }catch {
        case error:Exception =>{
          errorNums.add(1L)
          val outputStream=hlog.getOutPutStream()
          HDFSUtil.write2File(outputStream,map.toString+" because:"+error.getMessage)
        }
      }
    }
    sa.flush()
    sa.shutdown()
    hlog.close()
  }
}
