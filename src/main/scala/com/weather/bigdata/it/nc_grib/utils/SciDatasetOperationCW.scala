package com.weather.bigdata.it.nc_grib.utils

import com.weather.bigdata.it.nc_grib.ReadWrite.AttributesOperationCW
import com.weather.bigdata.it.nc_grib.core.{SciDatasetCW, VariableCW}
import ucar.ma2.DataType

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object SciDatasetOperationCW {


  private val defaultValues: Double = Constant.doubleDefault

  /**
    *
    * @param timeV
    * @param heightV
    * @param latV
    * @param lonV
    * @param dataName_dataTypes (dataName,datatypeStr)
    * @param datasetName
    * @param emptyAttribute
    * @return
    */
  def empty4SciDataset_DataType (timeV: VariableCW, heightV: VariableCW, latV: VariableCW, lonV: VariableCW, dataName_dataTypes: Array[(String, String)], datasetName: String, emptyAttribute: mutable.HashMap[String, String] = new mutable.HashMap[String, String]): SciDatasetCW = {
    val result = new SciDatasetCW(datasetName)
    result.insertAttributes(emptyAttribute)

    val timeName = timeV.name
    val heightName = heightV.name
    val latName = latV.name
    val lonName = lonV.name

    val timeLen = timeV.getLen( )
    val heightLen = heightV.getLen( )
    val latLen = latV.getLen( )
    val lonLen = lonV.getLen( )

    val dataLen = timeLen * heightLen * latLen * lonLen

    result.insertVariable(timeV.name, timeV)
    result.insertVariable(heightV.name, heightV)
    result.insertVariable(latV.name, latV)
    result.insertVariable(lonV.name, lonV)

    println("empty4SciDataset:time.Len=" + timeLen + ";height.Len=" + heightLen + ";lat.Len=" + latLen + ";lon.Len=" + lonLen + ";dataNames.Len=" + dataName_dataTypes.length)


    //    val length=timeLen*heightLen*latLen*lonLen

    dataName_dataTypes.foreach(dataName_dataType => {
      val dataName = dataName_dataType._1
      val dataTypeStr = dataName_dataType._2
      val data0: ucar.ma2.Array = {
        val shape = Array(timeLen, heightLen, latLen, lonLen)
        val len = shape.reduce((x, y) => x * y)
        DataType.getType(dataTypeStr) match {
          //          case DataType.INT => ucar.ma2.Array.factory(DataType.INT,Array(timeLen,heightLen,latLen,lonLen))
          case DataType.INT => ucar.ma2.Array.factory(DataType.INT, shape, Array.fill(len)(Constant.intDefault))
          case DataType.SHORT => ucar.ma2.Array.factory(DataType.SHORT, shape, Array.fill(len)(Constant.shortDefault))
          case DataType.FLOAT => ucar.ma2.Array.factory(DataType.FLOAT, shape, Array.fill(len)(Constant.floatDefault))
          case DataType.DOUBLE => ucar.ma2.Array.factory(DataType.DOUBLE, shape, Array.fill(len)(Constant.doubleDefault))
          case DataType.LONG => ucar.ma2.Array.factory(DataType.LONG, shape, Array.fill(len)(Constant.longDefault))
          case DataType.BYTE => ucar.ma2.Array.factory(DataType.BYTE, shape, Array.fill(len)(Constant.byteDefault))
          case badType => {
            throw new Exception("Can't convert ma2.Array[" + badType + "] to numeric array.")
            null
          }
        }
      }
      result.insertVariable(dataName, VariableOperationCW.creatVars4(data0, dataName, timeName, heightName, latName, lonName))

    })

    //    val vash=VariableOperation.allVars2NameVar4(this.getClass.getName,"", vals:ArrayBuffer[Variable], (timeName:String, heightName:String, latName:String, lonName:String))

    //    new SciDataset(vash,emptyAttribute,datasetName)
    result
  }

  def empty3SciDataset (timeV: VariableCW, latV: VariableCW, lonV: VariableCW, dataNames: Array[String], datasetName: String, datatypeStr: String, emptyAttribute: mutable.HashMap[String, String] = new mutable.HashMap[String, String]): SciDatasetCW = {
    val timeName = timeV.name
    val latName = latV.name
    val lonName = lonV.name

    val timeLen = timeV.dataDouble( ).length
    val latLen = latV.dataDouble( ).length
    val lonLen = lonV.dataDouble( ).length

    val dataLen = timeLen * latLen * lonLen

    val vals: ArrayBuffer[VariableCW] = new ArrayBuffer[VariableCW]

    vals.+=(timeV).+=(latV).+=(lonV)


    println("empty3SciDataset:time.Len=" + timeLen + ";lat.Len=" + latLen + ";lon.Len=" + lonLen)
    val data0: Array[Double] = new Array[Double](dataLen).map(f => defaultValues)
    dataNames.foreach(dataName => {
      val dataV = VariableOperationCW.creatVars3(data0, dataName, datatypeStr, timeName, latName, lonName, timeLen, latLen, lonLen)
      vals.+=(dataV)
    })

    //    val vash=VariableOperationCW.allVars2NameVar3(this.getClass.getName,"", vals:ArrayBuffer[VariableCW], (timeName:String,latName:String, lonName:String))

    new SciDatasetCW(vals, emptyAttribute, datasetName)
  }

  def empty3SciDataset_DataType (timeV: VariableCW, latV: VariableCW, lonV: VariableCW, dataName_dataTypes: Array[(String, String)], datasetName: String, emptyAttribute: mutable.HashMap[String, String] = new mutable.HashMap[String, String]): SciDatasetCW = {
    val result = new SciDatasetCW(datasetName)
    result.insertAttributes(emptyAttribute)

    val timeName = timeV.name
    val latName = latV.name
    val lonName = lonV.name

    val timeLen = timeV.getLen( )
    val latLen = latV.getLen( )
    val lonLen = lonV.getLen( )

    val dataLen = timeLen * latLen * lonLen

    result.insertVariable(timeV.name, timeV)
    result.insertVariable(latV.name, latV)
    result.insertVariable(lonV.name, lonV)

    println("empty3SciDataset:time.Len=" + timeLen + ";lat.Len=" + latLen + ";lon.Len=" + lonLen + ";dataNames.Len=" + dataName_dataTypes.length)


    dataName_dataTypes.foreach(dataName_dataType => {
      val dataName = dataName_dataType._1
      val dataTypeStr = dataName_dataType._2
      val data0: ucar.ma2.Array = {
        val shape = Array(timeLen, latLen, lonLen)
        val len = shape.reduce((x, y) => x * y)
        DataType.getType(dataTypeStr) match {
          //          case DataType.INT => ucar.ma2.Array.factory(DataType.INT,Array(timeLen,heightLen,latLen,lonLen))
          case DataType.INT => ucar.ma2.Array.factory(DataType.INT, shape, Array.fill(len)(Constant.intDefault))
          case DataType.SHORT => ucar.ma2.Array.factory(DataType.SHORT, shape, Array.fill(len)(Constant.shortDefault))
          case DataType.FLOAT => ucar.ma2.Array.factory(DataType.FLOAT, shape, Array.fill(len)(Constant.floatDefault))
          case DataType.DOUBLE => ucar.ma2.Array.factory(DataType.DOUBLE, shape, Array.fill(len)(Constant.doubleDefault))
          case DataType.LONG => ucar.ma2.Array.factory(DataType.LONG, shape, Array.fill(len)(Constant.longDefault))
          case DataType.BYTE => ucar.ma2.Array.factory(DataType.BYTE, shape, Array.fill(len)(Constant.byteDefault))
          case badType => {
            throw new Exception("Can't convert ma2.Array[" + badType + "] to numeric array.")
            null
          }
        }
      }
      result.insertVariable(dataName, VariableOperationCW.creatVars3(data0, dataName, timeName, latName, lonName))

    })

    //    val vash=VariableOperation.allVars2NameVar4(this.getClass.getName,"", vals:ArrayBuffer[Variable], (timeName:String, heightName:String, latName:String, lonName:String))

    //    new SciDataset(vash,emptyAttribute,datasetName)
    result
  }

  def empty2SciDataset (latV: VariableCW, lonV: VariableCW, dataNames: Array[String], datasetName: String, datatypeStr: String, emptyAttribute: mutable.HashMap[String, String] = new mutable.HashMap[String, String]): SciDatasetCW = {
    val latName = latV.name
    val lonName = lonV.name

    val latLen = latV.dataDouble( ).length
    val lonLen = lonV.dataDouble( ).length

    val dataLen = latLen * lonLen

    //    val emptyAttribute: mutable.HashMap[String,String]= new mutable.HashMap[String,String]

    val vals: ArrayBuffer[VariableCW] = new ArrayBuffer[VariableCW]

    vals.+=(latV).+=(lonV)

    //    val data0:Array[Double]=Array.ofDim[Double](timeLen*heightLen*latLen*lonLen)
    val data0: Array[Double] = new Array[Double](latLen * lonLen).map(f => defaultValues)

    dataNames.foreach(dataName => {
      val dataV = VariableOperationCW.creatVars2(data0, dataName, datatypeStr, latName, lonName, latLen, lonLen)
      vals.+=(dataV)
    })

    val vash = vals.map(p => (p.name, p))

    new SciDatasetCW(vash, emptyAttribute, datasetName)
  }

  def getDefaultValue: Double = this.defaultValues

//  def changeDataType (scidata: SciDatasetCW, dataType: String): SciDatasetCW = {
//    val (timeName, heightName, latName, lonName) = AttributesOperationCW.getVarName(scidata)
//    val timeLen: Int = scidata.variables.get(timeName).get.dataDouble( ).length
//    val heightLen: Int = scidata.variables.get(heightName).get.dataDouble( ).length
//    val latLen: Int = scidata.variables.get(latName).get.dataDouble( ).length
//    val lonLen: Int = scidata.variables.get(lonName).get.dataDouble( ).length
//    val fcType: String = AttributesOperationCW.getFcType(scidata)
//    val timeStep: Double = AttributesOperationCW.gettimeStep(scidata)
//    val dataNames: Array[String] = PropertiesUtil.getwElement(fcType, timeStep)
//    val dataType0: DataType = DataType.getType(dataType)
//    val dataVs: Array[VariableCW] = dataNames.map(dataName => {
//      val dataV0: VariableCW = scidata.variables.get(dataName).get
//      //      val dataName0=dataV0.dataType
//      val shape = dataV0.shape
//      val len = shape.reduce((x, y) => x * y)
//      val array: ucar.ma2.Array = ucar.ma2.Array.factory(dataType0, shape)
//
//      for (i <- 0 to len - 1) {
//        ucarma2ArrayOperationCW.setObject(array, i, dataV0.dataObject(i))
//      }
//
//      val dataV1: VariableCW = VariableOperationCW.creatVars4(array, dataName, timeName, heightName, latName, lonName)
//      dataV1
//    })
//
//    dataVs.foreach(dataV => {
//      val dataName = dataV.name
//      scidata.update(dataName, dataV)
//    })
//
//    scidata
//  }


  def empty4SciDataset (timeV: VariableCW, heightV: VariableCW, latV: VariableCW, lonV: VariableCW, dataNames: Array[String], datasetName: String, datatypeStr: String, emptyAttribute: mutable.HashMap[String, String] = new mutable.HashMap[String, String]): SciDatasetCW = {
    val timeName = timeV.name
    val heightName = heightV.name
    val latName = latV.name
    val lonName = lonV.name

    val timeLen = timeV.dataDouble.length
    val heightLen = heightV.dataDouble.length
    val latLen = latV.dataDouble.length
    val lonLen = lonV.dataDouble.length

    val dataLen = timeLen * heightLen * latLen * lonLen

    val vals: ArrayBuffer[VariableCW] = new ArrayBuffer[VariableCW]

    vals.+=(timeV).+=(heightV).+=(latV).+=(lonV)

    println("empty4SciDataset:time.Len=" + timeLen + ";height.Len=" + heightLen + ";lat.Len=" + latLen + ";lon.Len=" + lonLen)

    dataNames.foreach(dataName => {
      var data0: Array[Double] = new Array[Double](timeLen * heightLen * latLen * lonLen).map(f => defaultValues)
      vals.+=(VariableOperationCW.creatVars4(data0, dataName, timeName, heightName, latName, lonName, timeLen, heightLen, latLen, lonLen))
      data0 = null
    })

    //    val vash=VariableOperationCW.allVars2NameVar4(this.getClass.getName,"", vals:ArrayBuffer[VariableCW], (timeName:String, heightName:String, latName:String, lonName:String))

    new SciDatasetCW(vals, emptyAttribute, datasetName)
  }
}
