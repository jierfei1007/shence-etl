package com.facishare.fhc.test

/**
  * Created by jief on 2016/12/29.
  */

class Creature{
  val range:Int=10
  val env:Array[Int]=Array[Int](range)
}
class Ant extends Creature{
  override val range: Int = 2
}
object TestExetends {

  def main(args: Array[String]): Unit = {
    val ant = new Ant()
    println(ant.env(0))
  }
}
