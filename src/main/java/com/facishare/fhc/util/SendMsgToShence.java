package com.facishare.fhc.util;

import com.google.common.base.Splitter;
import com.google.common.base.Strings;

import com.github.autoconf.ConfigFactory;
import com.sensorsdata.analytics.javasdk.SensorsAnalytics;

import org.apache.commons.collections.map.HashedMap;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * <p>shen ce 数据处理</p>
 * Created by jief on 2016/12/22.
 */
public class SendMsgToShence {
  public static Set<String> shenceReservedWords = new HashSet();
  public static final String confMeta = "shence-config";
  private static Map<String, String> provMap = new HashMap();
  private static String dataDir;
  private static String dbUrl;
  private static String dbUser;
  private static String dbPasswd;
  private static String db;
  private static String dbTable;

  static {
    System.setProperty("spring.profiles.active", "foneshare");
    ConfigFactory.getInstance().getConfig(confMeta, e -> {
      String reservedWords = e.get("shenceReservedWords");
      Iterable<String> split = Splitter.on(",").trimResults().omitEmptyStrings().split(reservedWords);
      split.forEach(shenceReservedWords::add);
      dataDir = e.get("dataDir");
      dbUrl=e.get("dbUrl");
      dbUser=e.get("dbUser");
      dbPasswd=e.get("dbPasswd");
      db=e.get("db");
      dbTable=e.get("dbTable");
    });
  }
  public static void setProvInfo(){
    Connection conn;
    Statement stat;
    try{
      Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
      conn = DriverManager.getConnection(dbUrl, dbUser, dbPasswd);
      stat =conn.createStatement();
      String sql= String.format("select  prov,value from %s;",dbTable);
      ResultSet ret=stat.executeQuery(sql);
      String productVersion="";
      String versionName="";
      while(ret.next()){
        productVersion=ret.getString("prov");
        versionName=ret.getString("value");
        provMap.put(productVersion,versionName);
      }
      for(Map.Entry<String,String> ele:provMap.entrySet()){
        System.out.println("productVersion:"+ele.getKey()+" versionName:"+ele.getValue());
      }
      stat.close();
      conn.close();
    }catch(Exception e){
      throw new RuntimeException("db Exception:"+e.getMessage());
    }
  }
  public static void translate(Map<String, Object> data) {
    data.put("$time", data.getOrDefault("Time",new Date()));
    data.remove("Time");
    data.put("$ip", data.getOrDefault("IP", "127.0.0.1"));
    String key;
    String productVersion="";
    String versionName="";
    Set<String> keySet = new HashSet();
    keySet.addAll(data.keySet());
    for (String ele : keySet) {
      key = ele.toLowerCase();
      if (shenceReservedWords.contains(key)) {
        data.put("__" + ele, data.get(ele));
        data.remove(ele);
      } else if ("ProductVersion".equals(ele)) {
        if(data.get(ele)!=null) {
          productVersion = data.getOrDefault(ele,"").toString();
        }
        if(data.get("VersionName")!=null) {
          versionName = data.getOrDefault("VersionName","").toString();
        }
        if (!Strings.isNullOrEmpty(productVersion) && Strings.isNullOrEmpty(versionName)) {
          versionName = provMap.getOrDefault(productVersion,"");
          data.put("VersionName", versionName);
        }
      }
    }
  }
  /**
   * <p>发送map到神测</p>
   * @param sa
   * @param jsonMap
   * @throws Exception
   */
  public static void writeLog(SensorsAnalytics sa,Map<String,Object> jsonMap){
    String distinct_id = jsonMap.getOrDefault("EnterpriseID","0").toString();
    String eventName = jsonMap.getOrDefault("EventValue", "CEP_").toString();
//    Map<String,Object> newMap=new HashMap();
//    jsonMap.forEach((k,v)->{
//      if(k.equals("$time")){
//        newMap.put(k,new Date(Long.parseLong(v)));
//      }else if(k.equals("Platform")
//              ||k.equals("ServiceType")
//              ||k.equals("Duration")
//              || k.equals("EnterpriseID")
//              || k.equals("UserID")){
//        newMap.put(k,Integer.parseInt(v));
//      } else {
//        if(null==v){
//          v="";
//        }
//        newMap.put(k, v);
//      }
//    });
    try {
      sa.track(distinct_id, eventName, jsonMap);
    }catch(Exception e){
      throw new RuntimeException("writeLog error:"+jsonMap+"; errormsg="+e.getMessage());
    }
  }

}
