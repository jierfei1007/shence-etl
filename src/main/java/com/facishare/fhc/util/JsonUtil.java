package com.facishare.fhc.util;

import com.google.common.base.Preconditions;

import com.alibaba.fastjson.JSON;

/**
 * <p>fastjson 工具</p>
 * Created by jief on 2016/12/22.
 */
public class JsonUtil {

  public static String toJsonString(Object object){
    Preconditions.checkNotNull(object,"object is null");
    return JSON.toJSONString(object);
  }
}
