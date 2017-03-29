package com.facishare.fhc.test;

import com.sensorsdata.analytics.javasdk.SensorsAnalytics;
import com.sensorsdata.analytics.javasdk.exceptions.InvalidArgumentException;

import junit.framework.TestCase;

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
    sa.track(distinctId, "UserLogin" ,map);
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
   Map map= new HashMap();
    map.put("$time",new Date());
    map.put("EventValue","");
    map.put("Platform",-10000);
    map.put("DeviceID","");
    map.put("IP","127.0.0.1");
    map.put("Duration",-10000);
    map.put("ProductVersion","");
    map.put("$ip","127.0.0.1");
    map.put("LastActionName","");
    map.put("EnterpriseID",-10000);
    map.put("FullAction","");
    map.put("UserID",-10000);
    map.put("ServiceType",-10000);
    map.put("OSVersion","");
    map.put("VersionName","");
    map.put("BrowserVersion","");
    map.put("SecondActionName","");
    map.put("FullUserID",-10000);
    map.put("Browser","");
    map.put("FirstActionName","");
    map.put("param","{\"orientation\":\"portrait\"},https://www.fxiaoke.com/mob/salary/search.html?openUserId=FSUID_E4313DE2AE5D5D1141613B0F75B55B63&openCorpId=FSCID_2FF04BB9D5111A2E5713DAC38E0C873F&salaryDate=1480607999000&formType=577df790958a363a57ed5056&importId=58732b66e4b03d0d6d183665#/?_k=zzk1i8");
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
}
