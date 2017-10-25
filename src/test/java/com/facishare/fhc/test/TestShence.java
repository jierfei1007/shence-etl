package com.facishare.fhc.test;

import com.google.common.collect.Lists;

import com.facishare.fhc.util.SendMsgToShence;
import com.sensorsdata.analytics.javasdk.SensorsAnalytics;
import com.sensorsdata.analytics.javasdk.exceptions.InvalidArgumentException;

import junit.framework.TestCase;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * <p>神测</p>
 * Created by jief on 2016/12/22.
 */
public class TestShence extends TestCase{


  public void testsendShence()throws Exception{
    // 从 Sensors Analytics 获取的数据接收的 URL
    final String SA_SERVER_URL = "YOUR_SERVER_URL";

    // 使用 BatchConsumer 初始化 SensorsAnalytics
    SensorsAnalytics sa = new SensorsAnalytics(
            new SensorsAnalytics.BatchConsumer(SA_SERVER_URL, 50));

    // 用户的 Distinct Id
    String distinctId = "ABCDEF123456789";

    Map<String,Object> map=new HashMap<String,Object>();
    // 记录用户登录事件
    sa.track(distinctId,true,"UserLogin" ,map);
    // 程序结束前，停止 Sensors Analytics SDK 所有服务
    sa.shutdown();
  }

  public void testEventValue()throws Exception{
    Pattern KEY_PATTERN = Pattern.compile("^((?!^distinct_id$|^original_id$|^time$|^properties$|^id$|^first_id$|^second_id$|^users$|^events$|^event$|^user_id$|^date$|^datetime$)[a-zA-Z_$][a-zA-Z\\d_$]{0,99})$", 2);
    String key="aaaa_0_log";
    if(!KEY_PATTERN.matcher(key).matches()) {
      throw new InvalidArgumentException("The event value\'" + key + "\' is invalid.");
    }
  }
  public void test1(){
    String a="Thu Nov 17 16:51:40 CST 2016";

  }

  public void testvalid()throws Exception{
    DateFormat df= new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    Map map= new HashMap<String,Object>();
    map.put("EnterpriseID",259335);
    map.put("actionid","WorkPage_LeftNav_SelectFeedTask");
    map.put("httpxforwardedfor","259335");
//    map.put("sender","E.259335.1077");
//    map.put("app_id","FSAID_11490c83");
//    map.put("down_stream_users","");
//    map.put("$ip","172.17.32.157");
    map.put("user_agent","Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 UBrowser/6.2.3637.220 Safari/537.36");
    map.put("$time",df.parse("2017-08-16 17:42:07.33"));
    map.put("upstream_user_ids",Lists.newArrayList());
    System.out.println(map.toString());
    assertProperties("track",map);
  }
  private static void assertProperties(String eventType, Map<String, Object> properties) throws InvalidArgumentException {
    if(null != properties) {
      Iterator var3 = properties.entrySet().iterator();

      while(var3.hasNext()) {
        Map.Entry property = (Map.Entry)var3.next();
        if(!(property.getValue() instanceof Number) && !(property.getValue() instanceof Date) && !(property.getValue() instanceof String) && !(property.getValue() instanceof Boolean) && !(property.getValue() instanceof List)) {
          throw new InvalidArgumentException("-------------------The property value should be a basic type: Number, String, Date, Boolean, List<String>.");
        }

        if(((String)property.getKey()).equals("$time") && !(property.getValue() instanceof Date)) {
          throw new InvalidArgumentException("The property value of key \'$time\' should be a java.util.Date type.");
        }

        if(property.getValue() instanceof List) {
          ListIterator value = ((List)property.getValue()).listIterator();

          while(value.hasNext()) {
            Object element = value.next();
            if(!(element instanceof String)) {
              throw new InvalidArgumentException("+++++++++++++++++++The property value should be a basic type: Number, String, Date, Boolean, List<String>.");
            }

            if(((String)element).length() > 8191) {
              value.set(((String)element).substring(0, 8191));
              System.out.println(String.format("Element in property \'%s\' with LIST type is cut off while it\'s too long", new Object[]{(String)element}));
            }
          }
        }

        if(property.getValue() instanceof String) {
          String value1 = (String)property.getValue();
          if(value1.length() > 8191) {
            property.setValue(value1.substring(0, 8191));
            System.out.println(String.format("Property \'%s\' with STRING type is cut off while it\'s too long.", new Object[0], value1));
          }
        }

        if(eventType.equals("profile_increment")) {
          if(!(property.getValue() instanceof Number)) {
            throw new InvalidArgumentException("The property value of PROFILE_INCREMENT should be a Number.");
          }
        } else if(eventType.equals("profile_append") && !(property.getValue() instanceof List)) {
          throw new InvalidArgumentException("The property value of PROFILE_INCREMENT should be a List<String>.");
        }
      }

    }
  }

  public void testSendToShence()throws Exception{
    DateFormat df= new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    Map map= new HashMap<String,Object>();
    map.put("EnterpriseID",259335);
    map.put("image_text_param_id","2a4a474f07834aedb7a7abdfed2d4730");
    map.put("upstream_fs_ea","259335");
    map.put("sender","E.259335.1077");
    map.put("app_id","FSAID_11490c83");
    map.put("down_stream_users", Lists.newArrayList("E.543193.1000","E.424259.1061","E.fktest.4115","E.543193.1002","E.421080.1006","E.545931.1000","E.543277.1000","E.543193.1001","E.fktest.4352","E.543806.1000","E.421080.1002","E.421080.1001","E.421080.1000","E.424259.1056","E.fktest.1801","E.424259.1010","E.543193.1003","E.557197.1000","E.543163.1001","E.543163.1000","E.545024.1000","E.543161.1000","E.fktest.6601","E.fktest.4500","E.fktest.6600","E.543159.1000","E.fktest.1350","E.543164.1000","E.421080.1035","E.424259.1002","E.421080.1032","E.424259.1000","E.fktest.1796","E.543265.1001","E.543265.1000","E.424259.1009","E.543276.1000","E.424259.1004","E.555707.1000","E.540477.1000","E.545787.1000","E.545734.1000","E.fktest.2353","E.543803.1000","E.fktest.6599","E.fktest.6598","E.fktest.6597","E.fktest.2351","E.fktest.5187","E.424259.1030","E.516270.1001","E.543160.1000","E.554353.1000","E.544777.1000","E.543160.1001","E.516270.1003","E.544777.1001","E.556589.1000","E.543439.1000","E.516270.1000","E.fktest.1811","E.543162.1003","E.fktest.5737","E.543165.1000","E.fktest.5931","E.547686.1000"));
    map.put("$ip","172.17.32.157");
    map.put("messageId","d008e342-0eaf-4dfc-b6b5-3cac55063ab7");
    map.put("$time",df.parse("2017-08-16 17:42:07.33"));
    map.put("upstream_user_ids",Lists.newArrayList("1024","1025","1026","1027","1038","1039","1040","1043","1045","1046","1048","1049","1050","1051","1052","1053","1054","1055","1058","1059","1061","1062","1066","1067","1068","1069","1070","1071","1072","1073","1074","1075","1077","1081","1082","1083","1084","1085","1088","1089","1090","1092","1093","1096","1098","1099","1101","1102","1103","1106","1107","1113","1114","1115","1116","1117","1118","1119","1120","1121","1122","1123","1124","1000","1001","1002","1003","1004","1005","1006","1007","1011","1014","1015","1016","1019","1020","1021","1023"));
    SensorsAnalytics sa = new SensorsAnalytics(new SensorsAnalytics.BatchConsumer("http://sasdata.foneshare.cn/sa?"+"project=default", 1));
    sa.track("259335",true, "b_el_send_notice_daily", map);
    sa.flush();
  }


  public void testcrmSendToShence()throws Exception{
//    DateFormat df= new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
//    Map map= new HashMap<String,Object>();
//    map.put("_fcompanyid","405093");
//    map.put("$time",df.parse("2017-10-24 18:00:00.000"));
//    map.put("Value","test");
//    map.put("actionid","xhrerror");
//    SensorsAnalytics sa = new SensorsAnalytics(new SensorsAnalytics.BatchConsumer("http://sasdata.foneshare.cn/sa?"+"project=default", 1));
//    sa.track("405093",true ,"desktop_qx", map);
//    sa.flush();
     System.out.println(Integer.MAX_VALUE);
  }
}
