package com.weather.bigdata.it.nc_grib.core

import org.dia.utils.NetCDFUtils
import ucar.ma2.DataType
import ucar.nc2.dataset.NetcdfDataset
import ucar.nc2.{Attribute, Variable}

import scala.collection.mutable

class SciDatasetCW (val variables: mutable.HashMap[String, VariableCW], val attributes: mutable.HashMap[String, String], var datasetName: String) extends Serializable {

  def this (vars: Traversable[(String, VariableCW)], attr: Traversable[(String, String)], datasetName: String) {
    this(mutable.HashMap[String, VariableCW]( ) ++= vars, mutable.HashMap[String, String]( ) ++= attr, datasetName)
  }

  def this (vars: mutable.ArrayBuffer[(VariableCW)], attr: mutable.HashMap[String, String], datasetName: String) {
    this({
      val variables: mutable.HashMap[String, VariableCW] = new mutable.HashMap[String, VariableCW]
      vars.foreach(f => variables.put(f.name, f))
      variables
    }, attr, datasetName)
  }

  /*private def this(vars : Iterable[ucar.nc2.Variable], attr : Iterable[Attribute], datasetName : String) {
    this(
      vars.map(p =>{
        val v=new Variable(p)
        (p.getFullName,v )
      } ),
      attr.map(p => NetCDFUtils.convertAttribute(p)),
      datasetName
    )
  }*/

  def this (nvar: NetcdfDataset) {
    this({
      val varMap: mutable.HashMap[String, VariableCW] = new mutable.HashMap[String, VariableCW]( )
      val varsScala: java.util.List[Variable] = nvar.getVariables

      for (i <- 0 to varsScala.size( ) - 1) {
        val p: Variable = varsScala.get(i)
        val var2 = new VariableCW(p.getFullName, p)
        varMap.put(var2.name, var2)
      }
      varMap
    },
      {
        val attMap = new mutable.HashMap[String, String]
        val attrs: java.util.List[Attribute] = nvar.getGlobalAttributes
        for (i <- 0 to attrs.size( ) - 1) {
          val p = attrs.get(i)
          val (k, v) = NetCDFUtils.convertAttribute(p)
          attMap.put(k, v)
        }
        attMap
      },
      {
        val datasetName = nvar.getLocation.split("/").last
        datasetName
      }
    )

  }

  def this (nvar: NetcdfDataset, vars: scala.List[String]) {
    this({
      val varMap: mutable.HashMap[String, VariableCW] = new mutable.HashMap[String, VariableCW]( )
      vars.foreach(varName => {
        val p: ucar.nc2.Variable = nvar.findVariable(varName)
        val var2 = new VariableCW(p.getFullName, p)
        varMap.put(var2.name, var2)
      })
      varMap
    }, {
      val attMap = new mutable.HashMap[String, String]
      val attrs: java.util.List[Attribute] = nvar.getGlobalAttributes
      for (i <- 0 to attrs.size( ) - 1) {
        val p = attrs.get(i)
        val (k, v) = NetCDFUtils.convertAttribute(p)
        attMap.put(k, v)
      }
      attMap
    }, {
      val datasetName = nvar.getLocation.split("/").last
      datasetName
    })
  }

  def this (datasetName: String) {
    this(new mutable.HashMap[String, VariableCW]( ), new mutable.HashMap[String, String]( ), datasetName)
  }

  def setDataType (dataName: String, dataTypeStr: String): Unit = {
    this.variables.get(dataName).get.setDataType(dataTypeStr)
  }

  def setDataType (dataName: String, dataType: DataType): Unit = {
    this.variables.get(dataName).get.setDataType(dataType)
  }

  /**
    * Writes attribute in the form of key-value pairs
    *
    * @param metaDataVar tuple(s) of (attribute name, attribute value)
    * @return The modified SciDataset with added attributes
    */
  def insertAttributes (metaDataVar: (String, String)*): SciDatasetCW = {
    insertAttributes(metaDataVar)
    this
  }

  /**
    * Writes attribute in the form of key-value pairs in a collection.
    *
    * @param metaDataVars collection of tuple(s) of (attribute name, attribute value)
    * @return The modified SciDataset with added attributes
    */
  def insertAttributes (metaDataVars: Traversable[(String, String)]): SciDatasetCW = {
    attributes ++= metaDataVars
    this
  }

  /**
    * Writes variables in the form of key-value pairs
    *
    * @param variables collection of tuple(s) of (variable name, variable object)
    * @return The modified SciDataset with added attributes
    */
  def insertVariable (variables: (String, VariableCW)*): SciDatasetCW = {
    for ((k, v) <- variables) this.variables += ((k, v))
    this
  }

  /**
    * Writes a new variable to the hashmap.
    * It is recommended to use the update function instead
    * which enables you to use the "=" operator to insert new variables.
    *
    * @param key   name of variable
    * @param value the variable object ot insert
    * @return the current SciDataset
    */
  def insertVariable (key: String, value: VariableCW): SciDatasetCW = {
    this.variables(key) = value
    this
  }

  /**
    * Assigns a new name to the Dataset
    *
    * @param newName name to assign Dataset to
    * @return the renamed SciDataset
    */
  def setName (newName: String): SciDatasetCW = {
    datasetName = newName
    this
  }

  def apply (key: String): VariableCW = variables(key)

  def updataVariables (variables: mutable.HashMap[String, VariableCW]): SciDatasetCW = {
    variables.foreach(f => {
      this.update(f._1, f._2)
    })
    this
  }

  /**
    * Updates the SciDataset by inserting a new Variable
    * for the given key.
    *
    * Usage sciD("key") = variable
    *
    * @param key      the variable key to rename to
    * @param variable the variable
    * @return the modifed SciDataset
    */
  def update (key: String, variable: VariableCW): SciDatasetCW = {
    variables(key) = variable
    this
  }

  def updataAttributes (attributes: mutable.HashMap[String, String]): SciDatasetCW = {
    attributes.foreach(f => {
      this.update(f._1, f._2)
    })
    this
  }

  /**
    * Updates the SciDataset by inserting a new attribute
    * for the given key.
    *
    * Usage sciD("key") = attributeString
    *
    * @param key       the variable key to rename to
    * @param attribute the attribute name
    * @return the modified SciDataset
    */
  def update (key: String, attribute: String): SciDatasetCW = {
    attributes(key) = attribute
    this
  }

  /**
    * Access attribute values.
    * In Python's netcdf variable, attributes can be accessed as
    * members of classes like so:
    *    variable.attribute1
    * In scala we can't do that so we access attributes in
    * datasets like so:
    * dataset.attr("attribute")
    *
    * @param key the attribute name
    * @return the attribute value
    */
  def attr (key: String): String = attributes(key)

  //  def writeToNetCDF (name: String = datasetName, path: String = "", reftimeOpen: Boolean): Unit = {
  //    //    loadDependency.loadNetcdf4()
  //    this.writeToNetCDF3(name, path: String, reftimeOpen)
  //  }
  //
  //  private def writeToNetCDF3 (name: String, path0: String, reftimeOpen: Boolean): Boolean = {
  //    val netCDFversion = PropertiesUtil.writeCDFVersion
  //    val fileName: String = {
  //      val path: String = {
  //        if (PropertiesUtil.isPrd) {
  //          path0
  //        } else {
  //          if (path0.startsWith("/")) {
  //            path0.substring(1)
  //          } else {
  //            path0
  //          }
  //        }
  //      }
  //      path + name
  //    }
  //    val writer: NetcdfFileWriter = {
  //      val chunker: Nc4Chunking = Nc4ChunkingStrategy.factory(Nc4Chunking.Strategy.standard, 5, false)
  //      //      val chunker: Nc4Chunking=null
  //      if ("netcdf4".equals(netCDFversion)) {
  //        //NetCDF-4 C library loaded (jna_path='null', libname='netcdf').
  //
  //        NetcdfFileWriter.createNew(NetcdfFileWriter.Version.netcdf4, fileName, chunker)
  //      } else if ("ncstream".equals(netCDFversion)) {
  //        NetcdfFileWriter.createNew(NetcdfFileWriter.Version.ncstream, fileName, chunker)
  //      } else if ("netcdf3c".equals(netCDFversion)) {
  //        NetcdfFileWriter.createNew(NetcdfFileWriter.Version.netcdf3c, fileName, chunker)
  //      } else if ("netcdf3c64".equals(netCDFversion)) {
  //        NetcdfFileWriter.createNew(NetcdfFileWriter.Version.netcdf3c64, fileName, chunker)
  //      } else if ("netcdf4_classic".equals(netCDFversion)) {
  //        NetcdfFileWriter.createNew(NetcdfFileWriter.Version.netcdf4_classic, fileName, chunker)
  //      } else if ("netcdf3".equals(netCDFversion)) {
  //        NetcdfFileWriter.createNew(NetcdfFileWriter.Version.netcdf3, fileName, chunker)
  //      } else {
  //        NetcdfFileWriter.createNew(NetcdfFileWriter.Version.netcdf3, fileName, chunker)
  //      }
  //    }
  //
  //    writer.setLargeFile(true)
  //
  //    try {
  //      val writeNcStart = System.currentTimeMillis( )
  //
  //      for ((key, attribute) <- attributes) {
  //        writer.addGroupAttribute(null, new Attribute(key, attribute))
  //      }
  //      val globalDimensionMap = mutable.HashMap[String, ucar.nc2.Dimension]( )
  //
  //
  //      println(new Date + ";netcdfKeyValue前")
  //      val netcdfKeyValue: mutable.HashMap[ucar.nc2.Variable, ucar.ma2.Array] = variables.map {
  //        case (key: String, variable: VariableCW) => {
  //          val dims: java.util.ArrayList[ucar.nc2.Dimension] = new java.util.ArrayList[ucar.nc2.Dimension]( )
  //          val vdims: scala.List[(String, Int)] = variable.dims
  //          vdims.foreach(k => {
  //            val (dimName, length) = k
  //            dims.add(globalDimensionMap.getOrElseUpdate(dimName, writer.addDimension(null, dimName, length)))
  //          })
  //
  //          val dataType = DataType.getType(variable.dataType)
  //
  //          val varT: ucar.nc2.Variable = writer.addVariable(null, key, dataType, dims)
  //          variable.attributes.foreach(p => {
  //            varT.addAttribute(new ucar.nc2.Attribute(p._1, p._2))
  //          })
  //          val array = variable.array
  //          (varT, array)
  //        }
  //      }
  //      //reftime添加
  //      /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  //      val (reftimeVariable, reftimeArray) = this.addReftime(writer, reftimeOpen)
  //      /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  //
  //      writer.create( )
  //
  //      //写reftime
  //      this.writeReftime(reftimeOpen, writer, reftimeVariable, reftimeArray)
  //
  //
  ////      SystemGC.gc( )
  //      println(new Date + ";writer.write前")
  //      netcdfKeyValue.foreach(k => {
  //        val (variable: ucar.nc2.Variable, array: ucar.ma2.Array) = k
  //        //        writer.write(variable,array)
  //        try {
  //          writer.write(variable, array)
  //        } catch {
  //          case e: Exception => {
  //            val e0 = new Exception(e + ";variable.getFullName=" + variable.getFullName + ";array.dataType=" + array.getDataType + ";array.shape=" + ShowUtil.ArrayShowStr(array.getShape) + ";dims=" + variable.getDimensionsAll)
  //            e0.printStackTrace( )
  //          }
  //        }
  //
  //      })
  //
  //
  //      println(new Date + ";writer.write后")
  //
  //
  //      val writeNcEnd = System.currentTimeMillis( )
  //      WeatherShowtime.showDateStrOut1("writeNc", writeNcStart, writeNcEnd, "写文件的netcdf版本为:" + writer.getVersion + ";fileName=" + fileName + ";")
  //      true
  //    } catch {
  //      case e: Exception => {
  //        e.printStackTrace( )
  //        false
  //      }
  //    } finally {
  //      writer.close( )
  //    }
  //  }

  //  private def writeReftime(writer:NetcdfFileWriter,variable_add:ucar.nc2.Variable,array_add:ucar.ma2.Array): Unit ={
  //    if(PropertiesUtil.reftimeOpem){
  //      writer.write(variable_add,array_add)
  //    }
  //  }

  //  private def addReftime (writer: NetcdfFileWriter, reftimeOpen: Boolean = false): (ucar.nc2.Variable, ucar.ma2.Array) = {
  //    if (reftimeOpen && AttributesOperationCW.hasfcdate(this.attributes)) {
  //
  //      //        writer.setRedefineMode(true)
  //      val reftimeKey = "reftime"
  //      val sdf: SimpleDateFormat = new SimpleDateFormat("yyyyMMddHHmm")
  //      val fcdate = AttributesOperationCW.getfcdate(this.attributes)
  //      val timeStep = AttributesOperationCW.gettimeStep(this)
  //      val timeName = PropertiesUtil.timeName
  //      val timeV = this.variables.get(timeName).get
  //      val timeLen = timeV.getLen( )
  //      val times = ArrayOperation.ArithmeticArray(timeStep, timeLen, timeStep)
  //      val dims_add: util.ArrayList[Dimension] = new util.ArrayList[Dimension]( )
  //      val VDim: Dimension = writer.addDimension(null, reftimeKey, timeLen)
  //      dims_add.add(VDim)
  //      val variable_add: ucar.nc2.Variable = writer.addVariable(null, reftimeKey, DataType.STRING, dims_add)
  //      val sd: scala.Array[String] = StringDate.ArithmeticArrayDate_Hour(sdf, fcdate, times)
  //      val array_add: ucar.ma2.Array = ucar.ma2.Array.factory(DataType.STRING, scala.Array(timeLen), sd)
  //
  //      (variable_add, array_add)
  //    } else {
  //      (null, null)
  //    }
  //  }
  //
  //  private def writeReftime (reftimeOpen: Boolean = false, writer: NetcdfFileWriter, variable_add: ucar.nc2.Variable, array_add: ucar.ma2.Array): Unit = {
  //    if (reftimeOpen) {
  //      try {
  //        writer.write(variable_add, array_add)
  //        println("reftime写入")
  //      } catch {
  //        case e: Exception => {
  //          e.printStackTrace( )
  //        }
  //      }
  //
  //    }
  //  }

  /**
    * Creates a clone of the SciDataset
    *
    * @return
    */
  override def clone (): SciDatasetCW = {
    val newVariables: mutable.HashMap[String, VariableCW] = new mutable.HashMap[String, VariableCW]( )
    variables.foreach(f => {
      val variable: VariableCW = f._2.copy( )
      newVariables.put(f._1, variable)
    })
    val newAttributes = new mutable.HashMap[String, String]( )
    attributes.foreach(f => {
      newAttributes.put(f._1, f._2)
    })
    val newdatasetName = this.datasetName
    val newSci = new SciDatasetCW(newVariables, newAttributes, newdatasetName)
    newSci
  }

  /*def copy(): SciDataset = {
    /**
      * Hashmaps by default in scala do not do a deep clone.
      * Cloning the variable hashmap only copies the references
      * not the actual objects.
      *
      * Instead each variable in the hashmap is cloned.
      */
    val clonedVariables = variables.map({case (name, variable) => (name, variable.copy())})
    new SciDataset(clonedVariables, attributes.clone.toSeq, datasetName)
  }*/

  override def toString: String = datasetName

  override def equals (any: Any): Boolean = {
    val dataset = any.asInstanceOf[SciDatasetCW]

    dataset.datasetName == this.datasetName &&
      dataset.attributes == this.attributes &&
      dataset.variables == this.variables
  }

  override def hashCode (): Int = super.hashCode( )
}
