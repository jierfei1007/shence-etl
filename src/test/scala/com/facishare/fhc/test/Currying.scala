package com.facishare.fhc.test

/**
  * Created by jief on 2017/1/4.
  */
object Currying extends App{

  def add(x:Int)(y:Int)  = x + y


  def strcat(s1: String)(s2: String) = {
    s1 + s2
  }

  val str1:String = "Hello, "
  val str2:String = "Scala!"
  println( "str1 + str2 = " +  strcat(str1)(str2) )
}
