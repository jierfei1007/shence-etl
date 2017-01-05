package com.facishare.fhc.test

/**
  * Created by jief on 2017/1/4.
  */
object NiMingHanShu extends App{

  var inc =  (x:Int)  => x+1

  def add2 = new Function1[Int,Int]{

    def apply(x:Int):Int = x+1

  }

  var userDir =  ()  =>  {  System.getProperty("user.dir")  }
  println(userDir)
}
