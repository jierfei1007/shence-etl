package com.facishare.fhc.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}

private[util] class HDFSLog(uri: String) {

  private var outputSteam: FSDataOutputStream = null

  /**
    * get hdfs outPutStream
    *
    * @return
    */
  def getOutPutStream(): FSDataOutputStream = {
    if (outputSteam != null) {
      outputSteam
    } else {
      val fs = FileSystem.newInstance(new Configuration())
      val path = new Path(uri)
      if (!fs.exists(path)) {
        val fileOut: FSDataOutputStream = fs.create(path, true)
        outputSteam = fileOut
      } else {
        val fileOut: FSDataOutputStream = fs.append(path)
        outputSteam = fileOut
      }
      outputSteam
    }
  }
  /**
    * close outputStream
    */
  def close (): Unit = {
    if (outputSteam != null) {
      outputSteam.close()
    }
  }
}

/**
  * Created by jief on 2017/1/17.
  */
object HDFSLogFactory {

  def getHDFSLog(uri: String): HDFSLog = {
    new HDFSLog(uri)
  }

}
