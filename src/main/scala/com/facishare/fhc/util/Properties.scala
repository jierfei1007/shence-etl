package com.facishare.fhc.util

import com.github.autoconf.ConfigFactory

/**
  * Created by jief on 2017/6/26.
  */
object Properties {
  /**
    * 从配置文件中获取配置
    *
    * @param name
    * @param group
    */
  def setProperty(name: String, group: String): java.util.Map[String, String] = {
    System.setProperty("spring.profiles.active", group)
    val propConfig = ConfigFactory.getConfig(name).getAll()
    propConfig
  }
}
