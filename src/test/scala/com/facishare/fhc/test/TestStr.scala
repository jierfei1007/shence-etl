package com.facishare.fhc.test

import junit.framework.TestCase

import scala.util.matching.Regex

/**
  * Created by jief on 2016/12/21.
  */
class TestStr extends TestCase {

  def testregx(): Unit = {
    var uri: String = "/DemoAccount/958165@"
    uri = "/Dump.aspx"
    uri = "/H/Approve/SendReply"
    uri = "/StaticFiles/"
    //uri="/StaticFiles/喇喇/WEB-INF/web.xml"
//    uri="/WebReg/GetAreaCode.orig"
    //uri="/a/android/Account02/sendMobileBindingSMSCode"
    //uri="/a/iphone/Feed/Reply"
    uri="/scripts/openflashchart/open-flash-chart/php-ofc-library/ofc_upload_image.php/sssss/ssss/sss"
    //uri="FHE/EM1AAV/AVConversation/UpdateMultiAVConversation"
    //uri="FHE/EM1HTrain/courseMobCtrl/approveCourse"
    //uri="FRL/WebReg/Register/Init"
    //uri="XV/Invite/SendSMSCode"
    val CEP = "CEP_"
    var event_value = ""
    var firstActionName = ""
    var secondActionName = ""
    var lastActionName = ""

    val regex = new Regex("""^/(\w|\W)+""")
    if (regex.pattern.matcher(uri).matches()) {
      uri = uri.substring(1)
    }
    val action_array: Array[String] = uri.split('/')
//    if (action_array.size < 5) {
//      val first_action = action_array(0)
//      first_action.toUpperCase match {
//        case "A" => {
//          if (action_array.size > 3) {
//            event_value = CEP + action_array(0) + "_" + action_array(1) + "_" + action_array(2)
//            lastActionName = action_array(action_array.size - 1)
//          } else {
//            event_value = CEP + action_array.mkString("_")
//          }
//        }
//        case "H" => {
//          println("first H")
//          if (action_array.size > 2) {
//            event_value = CEP + action_array(0) + "_" + action_array(1)
//            lastActionName = action_array(action_array.size - 1)
//          } else {
//            event_value = CEP + action_array.mkString("_")
//          }
//        }
//        case "FHH" => {
//          println("first FHH")
//          if (action_array.size > 3) {
//            event_value = CEP + action_array(0) + "_" + action_array(1)
//            firstActionName =event_value + "_" + action_array(2)
//            lastActionName = action_array(action_array.size - 1)
//          } else {
//            event_value = CEP + action_array.mkString("_")
//          }
//        }
//        case "FHE" => {
//          println("first FHE")
//          if (action_array.size > 3) {
//            event_value = CEP + action_array(0) + "_" + action_array(1)
//            firstActionName = event_value + "_" + action_array(2)
//            lastActionName = action_array(action_array.size - 1)
//          } else {
//            event_value = CEP + action_array.mkString("_")
//          }
//        }
//        case "FRL" => {
//          println("first FRL")
//          if (action_array.size > 3) {
//            event_value = CEP + action_array(0) + "_" + action_array(1)
//            firstActionName = event_value + "_" + action_array(2)
//            lastActionName = action_array(action_array.size - 1)
//          } else {
//            event_value = CEP + action_array.mkString("_")
//          }
//        }
//        case "XV" => {
//          println("first XV")
//          if (action_array.size > 2) {
//            event_value = CEP + action_array(0) + "_" + action_array(1)
//            lastActionName = action_array(action_array.size - 1)
//          } else {
//            event_value = CEP + action_array.mkString("_")
//          }
//        }
//        case _ => {
//          println("first others")
//          event_value = CEP + action_array.mkString("_")
//        }
//      }
//    } else {
//      event_value = CEP + action_array(0) + "_" + action_array(1)
//      firstActionName = event_value + "_" + action_array(2)
//      secondActionName = firstActionName + "_" + action_array(3)
//      lastActionName = action_array(action_array.size - 1)
//    }
//    val actions = (event_value, firstActionName, secondActionName, lastActionName)
//    println(actions.toString)
    println(mkevents(action_array))

  }


  def mkevents(actionArray:Array[String]):Tuple4[String,String,String,String]= {
    val CEP = "CEP"
    var event_value = ""
    var firstActionName = ""
    var secondActionName = ""
    var lastActionName = ""

    actionArray.size match {
      case 1 =>{
        event_value = CEP + "_"+ actionArray.mkString("_")
      }
      case 2 =>{
        event_value = CEP + "_"+ actionArray.mkString("_")
      }
      case 3 =>{
        if(actionArray(0).equalsIgnoreCase("a")){
          event_value = CEP + "_"+ actionArray.mkString("_")
        }else{
          event_value = CEP +"_"+ actionArray(0)+"_"+actionArray(1)
          lastActionName=actionArray(2)
        }
      }
      case 4 =>{
        if(actionArray(0).equalsIgnoreCase("a")){
          event_value = CEP +"_"+ actionArray(0)+"_"+actionArray(1)+"_"+actionArray(2)
          lastActionName=actionArray(3)
        }else{
          event_value = CEP +"_"+ actionArray(0)+"_"+actionArray(1)
          firstActionName=event_value+"_"+actionArray(2)
          lastActionName=actionArray(3)
        }
      }
      case _=>{
        event_value = CEP +"_"+ actionArray(0)+"_"+actionArray(1)
        firstActionName=event_value+"_"+actionArray(2)
        secondActionName=firstActionName+"_"+actionArray(3)
        lastActionName=actionArray(actionArray.size-1)

      }
    }
    (event_value, firstActionName, secondActionName, lastActionName)
  }
}