package com.weather.bigdata.it.nc_grib.ReadWrite

import java.net.InetAddress
import java.util.Date

import com.alibaba.fastjson.JSONObject
import com.weather.bigdata.it.nc_grib.core.{SciDatasetCW, VariableCW}
import com.weather.bigdata.it.utils.formatUtil.DateFormatUtil

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable

object AttributesOperationCW {
  private def getIp():String={
    val ip:String=InetAddress.getLocalHost.getHostAddress
    ip
  }

  def addExecutorIDTrace (att: mutable.HashMap[String, String]): mutable.HashMap[String, String] = {
    val ExeID: String = this.getIp( )
    addInfo(att: mutable.HashMap[String, String], AttributesConstant.executorIDtraceKey, ExeID: String)
  }

  def getExecutorIDTrace (att: mutable.HashMap[String, String]): String = {
    att.get(AttributesConstant.executorIDtraceKey).get
  }

  def addExecutorIDTrace (scidata: SciDatasetCW): SciDatasetCW = {
    //    val ExeID:String=SparkUtil.getExecutorId()
    val addStr0 = this.addExecutorIDTraceStr(scidata.attributes)
    scidata.update(AttributesConstant.executorIDtraceKey, addStr0)
    scidata
  }

  def addExecutorIDTraceStr (att: mutable.HashMap[String, String]): String = {
    val ExeID: String = this.getIp( )
    this.addInfoStr(att, AttributesConstant.executorIDtraceKey, ExeID)
  }

  def addAppIDTrace (att: mutable.HashMap[String, String], AppID: String): mutable.HashMap[String, String] = {
    addInfo(att: mutable.HashMap[String, String], AttributesConstant.appIDtraceKey, AppID: String)
  }

  def addInfo (att: mutable.HashMap[String, String], key: String, value: String): mutable.HashMap[String, String] = {
    val value_new = this.addInfoStr(att, key, value)
    att.put(key, value_new)
    att
  }

  def getAppIDTrace (att: mutable.HashMap[String, String]): String = {
    att.get(AttributesConstant.appIDtraceKey).get
  }

  def addAppIDTrace (scidata: SciDatasetCW, AppID: String): SciDatasetCW = {
    val addStr0 = this.addAppIDTraceStr(scidata.attributes, AppID)
    scidata.update(AttributesConstant.appIDtraceKey, addStr0)
    scidata
  }

  /*def addAppId(jsonParam:JSONObject): JSONObject ={
    val AppId=ContextUtil.getApplicationID
    this.addAppId(jsonParam,AppId)
  }*/

  def addAppIDTraceStr (att: mutable.HashMap[String, String], AppID: String): String = {
    this.addInfoStr(att, AttributesConstant.appIDtraceKey, AppID)
  }

  def addAppId (jsonParam: JSONObject, AppId: String): JSONObject = {
    jsonParam.put(AttributesConstant.appIdKey, AppId)
    jsonParam
  }

  def addTrace (att: mutable.HashMap[String, String], addStr: String): mutable.HashMap[String, String] = {
    addInfo(att: mutable.HashMap[String, String], AttributesConstant.traceKey, addStr: String)
  }

  def getTrace (att: mutable.HashMap[String, String]): String = {
    att.get(AttributesConstant.traceKey).get
  }

  def addTrace (scidata: SciDatasetCW, addStr: String): SciDatasetCW = {
    val addStr0 = this.addTraceStr(scidata.attributes, addStr)
    scidata.update(AttributesConstant.traceKey, addStr0)
    scidata
  }

  def addTraceStr (att: mutable.HashMap[String, String], addStr: String): String = {
    this.addInfoStr(att, AttributesConstant.traceKey, addStr)
  }

  def addInfo (scidata: SciDatasetCW, key: String, value: String): SciDatasetCW = {
    val addStr = this.addInfoStr(scidata.attributes, key, value)
    scidata.update(key, addStr)
    scidata
  }

  def addInfoStr (att: mutable.HashMap[String, String], key: String, value: String): String = {
    if (att.contains(key)) {
      val value_old: String = att.get(key).get
      value_old + "->" + value
    } else {
      value
    }
  }

  def addIDName (att: mutable.HashMap[String, String], idName: String): mutable.HashMap[String, String] = {
    att.put(AttributesConstant.idNameKey, idName)
    att
  }

  def updateIDName (scidata: SciDatasetCW, id: Int): SciDatasetCW = {
    //    val att=this.addIDName(scidata.attributes,id)
    scidata.update(AttributesConstant.idNameKey, id.toString)
  }

  def updateFcType (att: mutable.HashMap[String, String], fcType: String): mutable.HashMap[String, String] = {
    att.put(AttributesConstant.fcTypeKey, fcType)
    att
  }


  def updateFcType (scidata: SciDatasetCW, fcType: String): SciDatasetCW = {
    scidata.update(AttributesConstant.fcTypeKey, fcType)
  }

  def addFcType (jsonParam: JSONObject, fcType: String): JSONObject = {
    jsonParam.put(AttributesConstant.fcTypeKey, fcType)
    jsonParam
  }

  def updateTimeStep (att: mutable.HashMap[String, String], timeStep: Double): mutable.HashMap[String, String] = {
    att.put(AttributesConstant.timeStepKey, timeStep.toString)
    att
  }


  def addTimeStep (jsonParam: JSONObject, timeStep: Double): JSONObject = {
    jsonParam.put(AttributesConstant.timeStepKey, timeStep)
    jsonParam
  }

  def updateTimeStep (scidata: SciDatasetCW, timeStep: Double): SciDatasetCW = {
    scidata.update(AttributesConstant.timeStepKey, timeStep.toString)
  }


  def getIDName (scidata: SciDatasetCW): String = this.getIDName(scidata.attributes)

  def getIDName (att: mutable.HashMap[String, String]): String = {
    if (att.contains(AttributesConstant.idNameKey)) {
      att.get(AttributesConstant.idNameKey).get
    } else {
      val e: Exception = new Exception("不存在IDName,查询过程问题")
      e.printStackTrace( )
      ""
    }
  }

  def getFcType (scidata: SciDatasetCW): String = {
    try {
      this.getFcType(scidata.attributes)
    } catch {
      case e: Exception => {
        val e1: Exception = new Exception("scidata.name=" + scidata.datasetName + "没有找到fcType标记;" + e.toString)
        e1.printStackTrace( )
        ""
      }
    }
  }

  def getFcType (att: mutable.HashMap[String, String]): String = {
    att.get(AttributesConstant.fcTypeKey).get
  }

  def updatefcdate (att: mutable.HashMap[String, String], fcDate: Date): mutable.HashMap[String, String] = {
    val dateStr = DateFormatUtil.YYYYMMDDHHMMSSStr0(fcDate)
    this.coverInfo(att, AttributesConstant.dateKey, dateStr)
  }

  def updatefcdate (scidata: SciDatasetCW, fcDate: Date): SciDatasetCW = {
    val dateStr = DateFormatUtil.YYYYMMDDHHMMSSStr0(fcDate)
    scidata.attributes.update(AttributesConstant.dateKey, dateStr)
    scidata
  }

  def getfcdate (scidata: SciDatasetCW): Date = {
    this.getfcdate(scidata.attributes)
  }

  def getfcdate (att: mutable.HashMap[String, String]): Date = {
    val dateStr = att.get(AttributesConstant.dateKey).get
    DateFormatUtil.YYYYMMDDHHMMSS0(dateStr)
  }

  def hasfcdate (scidata: SciDatasetCW): Boolean = {
    this.hasfcdate(scidata.attributes)
  }

  def hasfcdate (att: mutable.HashMap[String, String]): Boolean = {
    att.contains(AttributesConstant.dateKey)
  }

  def addfcdate (jsonParam: JSONObject, fcDate: Date): JSONObject = {
    this.addApplicationDate(jsonParam, fcDate)
  }

  private def addApplicationDate (jsonParam: JSONObject, date: Date): JSONObject = {
    val dateStr = DateFormatUtil.YYYYMMDDHHMMSSStr0(date)
    jsonParam.put(AttributesConstant.dateKey, dateStr)
    jsonParam
  }

  def addobsdate (jsonParam: JSONObject, obsDate: Date): JSONObject = {
    this.addApplicationDate(jsonParam, obsDate)
  }

  def addocffile (jsonParam: JSONObject, ocffile: String): JSONObject = {
    this.addApplicationFile(jsonParam, ocffile)
  }

  private def addApplicationFile (jsonParam: JSONObject, fileStr: String): JSONObject = {
    jsonParam.put(AttributesConstant.ocffileKey, fileStr)
    jsonParam
  }

  def addiputKVdate (jsonParam: JSONObject, inputKVDate: Date): JSONObject = {
    this.addApplicationDate(jsonParam, inputKVDate)
  }

  def addbeforeName (att: mutable.HashMap[String, String], beforeName: String): mutable.HashMap[String, String] = {
    this.coverInfo(att, AttributesConstant.beforeNameKey, beforeName)
  }

  def coverInfo (att: mutable.HashMap[String, String], key: String, value: String): mutable.HashMap[String, String] = {
    att.put(key, value)
    att
  }

  def addbeforeName (scidata: SciDatasetCW, beforeName: String): SciDatasetCW = {
    scidata.update(AttributesConstant.beforeNameKey, beforeName)
    scidata
  }

  def addbeforeName(v: VariableCW, beforeName: String): VariableCW = {
    //    scidata.update(AttributesConstant.beforeNameKey, beforeName)
    v.update(AttributesConstant.beforeNameKey, beforeName)
    v
  }

  /*def addVarName(att:mutable.HashMap[String,String],timeName:String,heightName:String,latName:String,lonName:String): mutable.HashMap[String,String] ={
    att.put(this.timeNameKey,timeName)
    att.put(this.heightNameKey,heightName)
    att.put(this.latNameKey,latName)
    att.put(this.lonNameKey,lonName)
    att
  }
  def addVarName(scidata:SciDataset,timeName:String,heightName:String,latName:String,lonName:String): SciDataset ={
    scidata.update(this.timeNameKey,timeName)
    scidata.update(this.heightNameKey,heightName)
    scidata.update(this.latNameKey,latName)
    scidata.update(this.lonNameKey,lonName)
    scidata
  }*/
  def addVarName (att: mutable.HashMap[String, String], varsName: (String, String, String, String)): mutable.HashMap[String, String] = {
    //    val (timeName,heightName,latName,lonName)=varsName
    val timeName = varsName._1
    val heightName = varsName._2
    val latName = varsName._3
    val lonName = varsName._4
    att.put(AttributesConstant.timeNameKey, timeName)
    att.put(AttributesConstant.heightNameKey, heightName)
    att.put(AttributesConstant.latNameKey, latName)
    att.put(AttributesConstant.lonNameKey, lonName)
    att
  }


  def addVarName (scidata: SciDatasetCW, varsName: (String, String, String, String)): SciDatasetCW = {
    val timeName = varsName._1
    val heightName = varsName._2
    val latName = varsName._3
    val lonName = varsName._4
    scidata.update(AttributesConstant.timeNameKey, timeName)
    scidata.update(AttributesConstant.heightNameKey, heightName)
    scidata.update(AttributesConstant.latNameKey, latName)
    scidata.update(AttributesConstant.lonNameKey, lonName)
    scidata
  }

  def getVarName (scidata: SciDatasetCW): (String, String, String, String) = {
    val att = scidata.attributes
    val timeName: String = att.get(AttributesConstant.timeNameKey).get
    val heightName: String = att.get(AttributesConstant.heightNameKey).get
    val latName: String = att.get(AttributesConstant.latNameKey).get
    val lonName: String = att.get(AttributesConstant.lonNameKey).get
    (timeName, heightName, latName, lonName)
  }

  def getVarName_tlatlon (scidata: SciDatasetCW): (String, String, String) = {
    val att = scidata.attributes
    val timeName: String = att.get(AttributesConstant.timeNameKey).get
    val latName: String = att.get(AttributesConstant.latNameKey).get
    val lonName: String = att.get(AttributesConstant.lonNameKey).get
    (timeName, latName, lonName)
  }

  def addDataNames (scidata: SciDatasetCW, dataName: String): SciDatasetCW = {
    this.addDataNames(scidata, Array(dataName))
  }

  def addDataNames (scidata: SciDatasetCW, dataNames: Array[String]): SciDatasetCW = {
    val dataNames0: Array[String] = this.getDataNames(scidata)
    val dataNames1: ArrayBuffer[String] = new ArrayBuffer[String]( )

    dataNames0.foreach(dataName => dataNames1 += dataName)
    dataNames.foreach(dataName => dataNames1 += dataName)

    this.setDataNames(scidata, dataNames1.toArray)
  }

  def setDataNames (scidata: SciDatasetCW, dataNames: Array[String]): SciDatasetCW = {
    val dataNamesStr = dataNames.reduce((x, y) => (x + "," + y))
    scidata.update(AttributesConstant.dataNamesKey, dataNamesStr)
    scidata
  }

  def getDataNames (scidata: SciDatasetCW): Array[String] = {
    val attribute0 = scidata.attributes
    val dataNamesStr: String = {
      if (attribute0.contains(AttributesConstant.dataNamesKey)) {
        attribute0.get(AttributesConstant.dataNamesKey).get
      } else {
        ""
      }
    }
    if (dataNamesStr.equals("")) {
      Array.empty[String]
    } else {
      dataNamesStr.split(",")
    }
  }

  def clearDataNames (scidata: SciDatasetCW): SciDatasetCW = {
    val attribute0 = scidata.attributes
    if (attribute0.contains(AttributesConstant.dataNamesKey)) {
      attribute0.remove(AttributesConstant.dataNamesKey)
    }
    scidata
  }


  def addIDName (jsonParam: JSONObject, idName: String): JSONObject = {
    jsonParam.put(AttributesConstant.idNameKey, idName)
    jsonParam
  }

  def gettimeStep (scidata: SciDatasetCW): Double = this.gettimeStep(scidata.attributes)

  def gettimeStep (att: mutable.HashMap[String, String]): Double = att.get(AttributesConstant.timeStepKey).get.toDouble

  def creatAttribute (): mutable.HashMap[String, String] = mutable.HashMap("unit" -> "1")

  def fuseAttribute (arrAtt: Array[mutable.HashMap[String, String]]): mutable.HashMap[String, String] = {
    arrAtt.reduce((x, y) => this.fuseAttribute(x, y))
  }

  def fuseAttribute (x: mutable.HashMap[String, String], y: mutable.HashMap[String, String]): mutable.HashMap[String, String] = {
    x.++(y)
    x
  }

  def setReftimeOpenDefault (scidata: SciDatasetCW): Unit = {
    this.setReftimeOpen(scidata, false)
  }

  def setReftimeOpen (scidata: SciDatasetCW, reftimeOpen: Boolean): Unit = {
    scidata.attributes.put(AttributesConstant.reftimeOpemKey, reftimeOpen.toString)

    //    scidata
  }

  def getReftimeOpen (scidata: SciDatasetCW): Boolean = {
    val reftimeOpen: Boolean = {
      val attribute = scidata.attributes
      if (attribute.contains(AttributesConstant.reftimeOpemKey)) {
        attribute.get(AttributesConstant.reftimeOpemKey).get.toBoolean
      } else {
        false
      }
    }
    reftimeOpen
  }

  def addVarName_tlatlon (att: mutable.HashMap[String, String], varsName: (String, String, String)): mutable.HashMap[String, String] = {
    val timeName = varsName._1
    val latName = varsName._2
    val lonName = varsName._3
    att.put(AttributesConstant.timeNameKey, timeName)
    att.put(AttributesConstant.latNameKey, latName)
    att.put(AttributesConstant.lonNameKey, lonName)
    att
  }

  def addVarName_tlatlon (scidata: SciDatasetCW, varsName: (String, String, String)): SciDatasetCW = {
    val timeName = varsName._1
    val latName = varsName._2
    val lonName = varsName._3
    scidata.update(AttributesConstant.timeNameKey, timeName)
    scidata.update(AttributesConstant.latNameKey, latName)
    scidata.update(AttributesConstant.lonNameKey, lonName)
    scidata
  }

  def main (args: Array[String]): Unit = {

  }
}
