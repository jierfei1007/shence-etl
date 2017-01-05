package com.facishare.fhc.test

/**
  * Created by jief on 2017/1/4.
  */
object HanShuQianTao {

  def main(args: Array[String]): Unit = {
    println( factorial(0) )
    println( factorial(1) )
    println( factorial(2) )
    println( factorial(3) )
  }

  def factorial(i: Int): Int = {
    def fact(i: Int, accumulator: Int): Int = {
      if (i <= 1)
        accumulator
      else
        fact(i - 1, i * accumulator)
    }
    fact(i, 1)
  }
}
