package com.facishare.fhc.util

import org.apache.commons.lang.StringUtils
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Table}

/**
  * Created by jief on 2017/6/26.
  */
object HbaseCommonUtil {
  /**
    * 获取hbase表连接
    * @return
    */
  def getHbaseTable(propConfig: java.util.Map[String, String],HTableName:String,HFamilys:Array[String]):(Connection, Table) = {
    require(StringUtils.isNotEmpty(HTableName),"hbase table name is empty!")
    require(HFamilys!=null && HFamilys.length>0,"hbase table family can not empty!")
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.property.clientPort", propConfig.get("hbase.zk.clientPort"))
    conf.set("hbase.zookeeper.quorum", propConfig.get("hbase.zk.quorum"))
    //Connection 的创建是个重量级的工作，线程安全，是操作hbase的入口
    val conn = ConnectionFactory.createConnection(conf)
    //从Connection获得 Admin
    val admin = conn.getAdmin
    //本例将操作的表名
    val tableName = TableName.valueOf(HTableName)
    if (!admin.tableExists(tableName)){
      val tableDescriptor= new HTableDescriptor(tableName)
      HFamilys.filter(f=>StringUtils.isNotEmpty(f)).foreach(family=>{
        tableDescriptor.addFamily(new HColumnDescriptor(family.getBytes))
      })
      admin.createTable(tableDescriptor)
    }
    val table = conn.getTable(tableName)
    (conn, table)
  }

  /**
    * 获取hbase表连接
    * @return
    */
  def getExistHbaseTable(propConfig: java.util.Map[String, String],HTableName:String):(Connection, Table) = {
    require(StringUtils.isNotEmpty(HTableName),"hbase table name is empty!")
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.property.clientPort", propConfig.get("hbase.zk.clientPort"))
    conf.set("hbase.zookeeper.quorum", propConfig.get("hbase.zk.quorum"))
    //Connection 的创建是个重量级的工作，线程安全，是操作hbase的入口
    val conn = ConnectionFactory.createConnection(conf)
    //从Connection获得 Admin
    val admin = conn.getAdmin
    //本例将操作的表名
    val tableName = TableName.valueOf(HTableName)
    val table = conn.getTable(tableName)
    (conn, table)
  }

  /**
    * 获取hbase connection
    * @param propConfig
    * @return
    */
  def getHbaseConnection(propConfig: java.util.Map[String, String]):Connection={
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.property.clientPort", propConfig.get("hbase.zk.clientPort"))
    conf.set("hbase.zookeeper.quorum", propConfig.get("hbase.zk.quorum"))
    //Connection 的创建是个重量级的工作，线程安全，是操作hbase的入口
    val conn = ConnectionFactory.createConnection(conf)
    conn
  }

  /**
    * 关闭hbase链接
    * @param conn
    * @param table
    */
  def closeHbaseTable(conn: Connection, table: Table) = {

    if (table != null) {
      table.close()
    }
    if (conn != null) {
      conn.close()
    }
  }
}
