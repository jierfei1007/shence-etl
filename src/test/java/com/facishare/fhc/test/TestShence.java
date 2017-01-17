package com.facishare.fhc.test;

import com.sensorsdata.analytics.javasdk.SensorsAnalytics;
import com.sensorsdata.analytics.javasdk.exceptions.InvalidArgumentException;

import junit.framework.TestCase;

import java.util.HashMap;
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
}
