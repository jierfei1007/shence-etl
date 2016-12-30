package com.facishare.fhc.test;

import java.util.Map;

/**
 * Created by jief on 2016/12/22.
 */
public class User {
  private String name;
  private int age;

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public int getAge() {
    return age;
  }

  public void setAge(int age) {
    this.age = age;
  }

  public static void  testMap(Map<String,Object> map){
    map.forEach((k,v)->{System.out.println("k="+k+",v="+v);});
  }
}
