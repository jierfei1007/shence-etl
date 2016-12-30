package com.facishare.fhc.bean;

import java.io.Serializable;
import java.sql.Timestamp;

/**
 * <p>神测cep action数据bean</p>
 * Created by jief on 2016/12/21.
 */

public class ShenceCEPServerAction implements Serializable{

  private Integer eid;            //企业id
  private Integer employeeId;     //员工id
  private Integer platform;       //平台类型
  private String deviceId;
  private String employeeIp;
  private Integer serviceType;
  private String osVersion;
  private String browserVersion;
  private String browser;
  private Timestamp visitTime;
  private Integer duration;
  private String innerProVersion;
  private Timestamp time;
  private String eventValue;
  private String firstActionName;
  private String secondActionName;
  private String lastActionName;
  private String fullAction;

  public Integer getEid() {
    return eid;
  }

  public void setEid(Integer eid) {
    this.eid = eid;
  }

  public Integer getEmployeeId() {
    return employeeId;
  }

  public void setEmployeeId(Integer employeeId) {
    this.employeeId = employeeId;
  }

  public Integer getPlatform() {
    return platform;
  }

  public void setPlatform(Integer platform) {
    this.platform = platform;
  }

  public String getDeviceId() {
    return deviceId;
  }

  public void setDeviceId(String deviceId) {
    this.deviceId = deviceId;
  }

  public String getEmployeeIp() {
    return employeeIp;
  }

  public void setEmployeeIp(String employeeIp) {
    this.employeeIp = employeeIp;
  }

  public Integer getServiceType() {
    return serviceType;
  }

  public void setServiceType(Integer serviceType) {
    this.serviceType = serviceType;
  }

  public String getOsVersion() {
    return osVersion;
  }

  public void setOsVersion(String osVersion) {
    this.osVersion = osVersion;
  }

  public String getBrowserVersion() {
    return browserVersion;
  }

  public void setBrowserVersion(String browserVersion) {
    this.browserVersion = browserVersion;
  }

  public String getBrowser() {
    return browser;
  }

  public void setBrowser(String browser) {
    this.browser = browser;
  }

  public Timestamp getVisitTime() {
    return visitTime;
  }

  public void setVisitTime(Timestamp visitTime) {
    this.visitTime = visitTime;
  }

  public Integer getDuration() {
    return duration;
  }

  public void setDuration(Integer duration) {
    this.duration = duration;
  }

  public String getInnerProVersion() {
    return innerProVersion;
  }

  public void setInnerProVersion(String innerProVersion) {
    this.innerProVersion = innerProVersion;
  }

  public Timestamp getTime() {
    return time;
  }

  public void setTime(Timestamp time) {
    this.time = time;
  }

  public String getEventValue() {
    return eventValue;
  }

  public void setEventValue(String eventValue) {
    this.eventValue = eventValue;
  }

  public String getFirstActionName() {
    return firstActionName;
  }

  public void setFirstActionName(String firstActionName) {
    this.firstActionName = firstActionName;
  }

  public String getSecondActionName() {
    return secondActionName;
  }

  public void setSecondActionName(String secondActionName) {
    this.secondActionName = secondActionName;
  }

  public String getLastActionName() {
    return lastActionName;
  }

  public void setLastActionName(String lastActionName) {
    this.lastActionName = lastActionName;
  }

  public String getFullAction() {
    return fullAction;
  }

  public void setFullAction(String fullAction) {
    this.fullAction = fullAction;
  }
}
