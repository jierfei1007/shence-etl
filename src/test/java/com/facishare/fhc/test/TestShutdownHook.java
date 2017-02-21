package com.facishare.fhc.test;

/**
 * Created by jief on 2017/2/20.
 */
public class TestShutdownHook {

  public  static void main(String args[]){

    Runtime.getRuntime().addShutdownHook(new Thread() {
      public void run() {
        System.out.println("shutdown server hook !");
      }
    });

    System.out.println("end");
  }
}
