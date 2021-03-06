package com.facishare.fhc.util

import java.io.{BufferedReader, IOException, InputStreamReader}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
import org.apache.hadoop.hdfs.DistributedFileSystem


/**
  * Created by jief on 2016/12/22.
  */
object HDFSUtil {

  val NEW_LINE:String="\n"

  /**
    * write string to hdfs as text file
    * @param fileOut
    * @param value
    */
  def write2File(fileOut: FSDataOutputStream, value:String):Unit={
    try {
      fileOut.write(value.toString.getBytes("UTF-8"))
      fileOut.write(NEW_LINE.getBytes("UTF-8"))
    }catch{
      case io: IOException =>{throw io
      }
      case _: Throwable => {println("Got some other kind of exception")
            throw new RuntimeException("Got some other kind of exception")
      }
    }
  }
  /**
    * get hdfs outPutStream
    * @param uri
    * @return
    */
    def getOutPutStream(uri:String):FSDataOutputStream={
    val fs = FileSystem.newInstance(new Configuration())
    val path=new Path(uri)
    if (!fs.exists(path)) {
      val fileOut: FSDataOutputStream = fs.create(path, true)
      fileOut
    } else {
      val fileOut: FSDataOutputStream = fs.append(path)
      fileOut
    }
  }

  /**
    * get append outPutStream
    * @param uri
    * @return
    */
  def getOutPutStreamAppend(uri:String):FSDataOutputStream={
    val fs = FileSystem.newInstance(new Configuration())
    val path=new Path(uri)
    if (!fs.exists(path)){
      throw new RuntimeException("path:"+path+"not exists")
    }
    val fileOut: FSDataOutputStream = fs.append(path)
    fileOut
  }

  /**
    * read text file
    * @param uri hdfs path
    * @return
    */
  def readHDFSTextFile(uri:String):String={
    val fs = FileSystem.newInstance(new Configuration()).asInstanceOf[DistributedFileSystem]
    val path=new Path(uri)
    if (!fs.exists(path)){
      throw new RuntimeException("path:"+path+"not exists")
    }
    val inputStream=fs.open(path,1024)
    val bufferReader = new BufferedReader(new InputStreamReader(inputStream))
    var txt=""
    var line:String=bufferReader.readLine()
    while (line!=null) {
      txt=txt+line+"\n"
      line=bufferReader.readLine()
    }
    inputStream.close()
    fs.close()
    txt
  }

  /**
    * close hdfs file outputStream
    * @param fileOut
    */
  def close(fileOut: FSDataOutputStream): Unit ={
    if(fileOut!=null){
      fileOut.close()
    }
  }
}
