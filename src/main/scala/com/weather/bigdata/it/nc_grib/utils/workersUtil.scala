//package com.weather.bigdata.it.nc_grib.utils
//
//import java.net.InetAddress
//import java.util.regex.{Matcher, Pattern}
//
//class workersUtil {
//
//  def getIp(): String ={
//    val ip:String=InetAddress.getLocalHost.getHostAddress
//    ip
//  }
//  def getIp(url:String):String={
//    //保留端口号
//    //    val re:String="((http|ftp|https)://)(([a-zA-Z0-9._-]+)|([0-9]{1,3}.[0-9]{1,3}.[0-9]{1,3}.[0-9]{1,3}))(([a-zA-Z]{2,6})|(:[0-9]{1,4})?)"
//    //除去端口号
//    val re:String="((http|ftp|https)://)(([a-zA-Z0-9._-]+)|([0-9]{1,3}.[0-9]{1,3}.[0-9]{1,3}.[0-9]{1,3}))(([a-zA-Z]{2,6})?)"
//    val str:String={
//      val pattern:Pattern=Pattern.compile(re)
//      val matcher:Matcher=pattern.matcher(url)
//      if(matcher.matches()){
//        url
//      }else{
//        val split2:Array[String]=url.split(re)
//        if(split2.length>1){
//          url.substring(0,url.length-split2(1).length)
//        }else{
//          split2(0)
//        }
//      }
//    }
//    str.replace("http://","")
//
//  }
//  def getHostName(): String ={
//    InetAddress.getLocalHost.getHostName
//  }
//}
//
//object workersUtil {
//  private val su = new workersUtil( )
//  def getIp(url:String): String =this.su.getIp(url)
//  def getIp(): String = this.su.getIp()
//  def getHostName():String=this.su.getHostName()
//
//}
