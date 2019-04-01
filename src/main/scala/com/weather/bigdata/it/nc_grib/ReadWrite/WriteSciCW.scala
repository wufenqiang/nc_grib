package com.weather.bigdata.it.nc_grib.ReadWrite

import java.text.SimpleDateFormat
import java.util.Date

import com.weather.bigdata.it.nc_grib.core.{SciDatasetCW, VariableCW}
import com.weather.bigdata.it.utils.{ShowUtil, WeatherShowtime}
import com.weather.bigdata.it.utils.formatUtil.StringDate
import com.weather.bigdata.it.utils.hdfsUtil.HDFSOperation1
import com.weather.bigdata.it.utils.operation.ArrayOperation
import ucar.ma2.DataType
import ucar.nc2.{Attribute, Dimension, NetcdfFileWriter}
import ucar.nc2.write.{Nc4Chunking, Nc4ChunkingStrategy}

import scala.collection.mutable
class WriteSciCW(val scidata:SciDatasetCW){
  
  def write (resultHdfsPath: String): Boolean = {

    //获取ExcutorID
    val scidata1 = AttributesOperationCW.addExecutorIDTrace(scidata)
    val tmp = PropertiesUtil.tmp

    val reftimeOpen = AttributesOperationCW.getReftimeOpen(scidata1)

    val flag0: Boolean = {
      try {
        this.writeToNetCDF(scidata.datasetName, tmp, reftimeOpen)
        true
      } catch {
        case e: Exception => {
          e.printStackTrace( )
          false
        }
      }
    }
    //      this.writeToNetCDF(p,p.datasetName, SourcePathStr)

    if (flag0) {
      //上传hdfs
      val sourceFileStr = tmp + scidata1.datasetName
      val flag = HDFSOperation1.copyfile(sourceFileStr, resultHdfsPath, true, true)

      flag
    } else {
      flag0
    }


  }


  def writeToNetCDF (name: String = scidata.datasetName, path: String = "", reftimeOpen: Boolean): Unit = {
    //    loadDependency.loadNetcdf4()
    this.writeToNetCDF3(name, path: String, reftimeOpen)
  }

  private def writeToNetCDF3 (name: String, path0: String, reftimeOpen: Boolean): Boolean = {
    val netCDFversion = PropertiesUtil.writeCDFVersion
    val fileName: String = {
      val path: String = {
        if (path0.startsWith("/")) {
          path0.substring(1)
        } else {
          path0
        }
      }
      path + name
    }
    val writer: NetcdfFileWriter = {
      val chunker: Nc4Chunking = Nc4ChunkingStrategy.factory(Nc4Chunking.Strategy.standard, 5, false)
      //      val chunker: Nc4Chunking=null
      if ("netcdf4".equals(netCDFversion)) {
        NetcdfFileWriter.createNew(NetcdfFileWriter.Version.netcdf4, fileName, chunker)
      } else if ("ncstream".equals(netCDFversion)) {
        NetcdfFileWriter.createNew(NetcdfFileWriter.Version.ncstream, fileName, chunker)
      } else if ("netcdf3c".equals(netCDFversion)) {
        NetcdfFileWriter.createNew(NetcdfFileWriter.Version.netcdf3c, fileName, chunker)
      } else if ("netcdf3c64".equals(netCDFversion)) {
        NetcdfFileWriter.createNew(NetcdfFileWriter.Version.netcdf3c64, fileName, chunker)
      } else if ("netcdf4_classic".equals(netCDFversion)) {
        NetcdfFileWriter.createNew(NetcdfFileWriter.Version.netcdf4_classic, fileName, chunker)
      } else if ("netcdf3".equals(netCDFversion)) {
        NetcdfFileWriter.createNew(NetcdfFileWriter.Version.netcdf3, fileName, chunker)
      } else {
        NetcdfFileWriter.createNew(NetcdfFileWriter.Version.netcdf3, fileName, chunker)
      }
    }

    writer.setLargeFile(true)

    try {
      val writeNcStart = System.currentTimeMillis( )

      for ((key, attribute) <- scidata.attributes) {
        writer.addGroupAttribute(null, new Attribute(key, attribute))
      }
      val globalDimensionMap = mutable.HashMap[String, ucar.nc2.Dimension]( )


      println(new Date + ";netcdfKeyValue前")
      val netcdfKeyValue: mutable.HashMap[ucar.nc2.Variable, ucar.ma2.Array] = scidata.variables.map {
        case (key: String, variable: VariableCW) => {
          val dims: java.util.ArrayList[ucar.nc2.Dimension] = new java.util.ArrayList[ucar.nc2.Dimension]( )
          val vdims: scala.List[(String, Int)] = variable.dims
          vdims.foreach(k => {
            val (dimName, length) = k
            dims.add(globalDimensionMap.getOrElseUpdate(dimName, writer.addDimension(null, dimName, length)))
          })

          val dataType = DataType.getType(variable.dataType)

          val varT: ucar.nc2.Variable = writer.addVariable(null, key, dataType, dims)
          variable.attributes.foreach(p => {
            varT.addAttribute(new ucar.nc2.Attribute(p._1, p._2))
          })
          val array = variable.array
          (varT, array)
        }
      }
      //reftime添加
      /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
      val (reftimeVariable, reftimeArray) = this.addReftime(writer, reftimeOpen)
      /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

      writer.create( )

      //写reftime
      this.writeReftime(reftimeOpen, writer, reftimeVariable, reftimeArray)


      //      SystemGC.gc( )
      println(new Date + ";writer.write前")
      netcdfKeyValue.foreach(k => {
        val (variable: ucar.nc2.Variable, array: ucar.ma2.Array) = k
        //        writer.write(variable,array)
        try {
          writer.write(variable, array)
        } catch {
          case e: Exception => {
            PropertiesUtil.log.error(e)
            val e0 = new Exception(e + ";variable.getFullName=" + variable.getFullName + ";array.dataType=" + array.getDataType + ";array.shape=" + ShowUtil.ArrayShowStr(array.getShape) + ";dims=" + variable.getDimensionsAll)
            e0.printStackTrace( )
          }
        }

      })


      println(new Date + ";writer.write后")


      val writeNcEnd = System.currentTimeMillis( )
      WeatherShowtime.showDateStrOut1("writeNc", writeNcStart, writeNcEnd, "写文件的netcdf版本为:" + writer.getVersion + ";fileName=" + fileName + ";")
      true
    } catch {
      case e: Exception => {
        e.printStackTrace( )
        false
      }
    } finally {
      writer.close( )
    }
  }

  //  private def writeReftime(writer:NetcdfFileWriter,variable_add:ucar.nc2.Variable,array_add:ucar.ma2.Array): Unit ={
  //    if(PropertiesUtil.reftimeOpem){
  //      writer.write(variable_add,array_add)
  //    }
  //  }
  private def addReftime (writer: NetcdfFileWriter, reftimeOpen: Boolean = false): (ucar.nc2.Variable, ucar.ma2.Array) = {
    if (reftimeOpen && AttributesOperationCW.hasfcdate(scidata.attributes)) {

      //        writer.setRedefineMode(true)
      val reftimeKey = "reftime"
      val sdf: SimpleDateFormat = new SimpleDateFormat("yyyyMMddHHmm")
      val fcdate = AttributesOperationCW.getfcdate(scidata.attributes)
      val timeStep = AttributesOperationCW.gettimeStep(scidata)
      val timeName = PropertiesUtil.timeName
      val timeV = scidata.variables.get(timeName).get
      val timeLen = timeV.getLen( )
      val times = ArrayOperation.ArithmeticArray(timeStep, timeLen, timeStep)
      val dims_add: java.util.ArrayList[Dimension] = new java.util.ArrayList[Dimension]( )
      val VDim: Dimension = writer.addDimension(null, reftimeKey, timeLen)
      dims_add.add(VDim)
      val variable_add: ucar.nc2.Variable = writer.addVariable(null, reftimeKey, DataType.STRING, dims_add)
      val sd: scala.Array[String] = StringDate.ArithmeticArrayDate_Hour(sdf, fcdate, times)
      val array_add: ucar.ma2.Array = ucar.ma2.Array.factory(DataType.STRING, scala.Array(timeLen), sd)

      (variable_add, array_add)
    } else {
      (null, null)
    }
  }
  private def writeReftime (reftimeOpen: Boolean = false, writer: NetcdfFileWriter, variable_add: ucar.nc2.Variable, array_add: ucar.ma2.Array): Unit = {
    if (reftimeOpen) {
      try {
        writer.write(variable_add, array_add)
        println("reftime写入")
      } catch {
        case e: Exception => {
          e.printStackTrace( )
        }
      }

    }
  }

  //  def writeSplit(scidata0: SciDatasetCW, timeName:String,resultHdfsPath0: String): Boolean ={
  //    val timeV=scidata0.apply(timeName)
  //    val times:Array[Double]=timeV.dataDouble()
  //    val scidatas:Array[SciDatasetCW]=times.map(time=>{
  //      val scidata:SciDatasetCW={
  //        SciDatasetOperationCW.
  //      }
  //
  //      val resultHdfsPath=resultHdfsPath0+"/"+time+"/"
  //      val flag:Boolean=this.write(scidata,resultHdfsPath)
  //      flag
  //    }).reduce((x,y)=>(x && y))
  //  }
}
object WriteSciCW {
  implicit def fromSciCW(scidata:SciDatasetCW):WriteSciCW=new WriteSciCW(scidata)
}
