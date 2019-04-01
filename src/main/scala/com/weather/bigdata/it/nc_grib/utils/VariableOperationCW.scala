package com.weather.bigdata.it.nc_grib.utils

//import org.apache.spark.rdd.RDD
import com.weather.bigdata.it.nc_grib.core.{SciDatasetCW, VariableCW}
import com.weather.bigdata.it.utils.operation.ArrayOperation
import ucar.ma2.DataType

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object VariableOperationCW {


  def getVar (vals: ArrayBuffer[VariableCW], Name: String): VariableCW = vals.filter(p => p.name.equals(Name)).last

  def getVar2List (vals: ArrayBuffer[VariableCW], Name: String): List[VariableCW] = this.getVar2List(vals.toIterable, Name)

  def getVar2List (vals: Iterable[VariableCW], Name: String): List[VariableCW] = vals.filter(_.name == Name).toList

  def getVars (vals: Iterable[VariableCW], Names: Array[String]): ArrayBuffer[VariableCW] = {
    val newV = new ArrayBuffer[VariableCW]
    for (i <- 0 to Names.length - 1) {
      val v = getVar(vals, Names(i))
      newV += v
    }
    newV
  }

  private def getVar (vals: Iterable[VariableCW], Name: String): VariableCW = vals.filter(p => p.name.equals(Name)).last

  def plusOtherVars (mainV: ArrayBuffer[VariableCW], scidata: SciDatasetCW, Names: Array[String]): ArrayBuffer[VariableCW] = {
    val newV = {
      if (mainV != null) {
        mainV
      } else {
        new ArrayBuffer[VariableCW]
      }
    }

    scidata.variables.filter(p => (!Names.contains(p._1))).foreach(p => {
      newV += p._2
    })
    /*val vals=scidata.variables.values
    vals.foreach(
      v => {
        var flag:Boolean=true
        for(i<-0 to Names.length-1){
          if(Names(i).equals(v.name)){
            flag=false
          }
        }
        if(flag){
          newV+=v
        }
      }
    )*/
    newV
  }

  /*def allVars2NameVar(IATypes:String,idName:Int,vals:ArrayBuffer[Variable],timeName:String,heightName:String,latName:String,lonName:String): ArrayBuffer[(String,Variable)] ={
    val varHash = vals.map(
      p => {
        val pname=p.name
        val pdata=p.data()
        if(pname==timeName || pname==heightName){
          print("Process="+IATypes+";idName="+idName+";"+pname+ ".length=" + p.shape( )(1)+","+pname+"="+"")
          for(i <- 0 to pdata.length-2){
            print(pdata(i)+",")
          }
          println(pdata(pdata.length-1))
        }else if(pname==latName || pname==lonName){
          val latlonStep:Double=DataFormatUtil.doubleFormat(pdata(1)-pdata(0))
//          println("pdata(1)="+pdata(1)+";pdata(0)="+pdata(0))
          println("Process="+IATypes+";idName="+idName+";"+pname+ ".length=" + p.shape( )(1)+","+pname+"="+pdata(0)+"-"+pdata(pdata.length-1)+",step="+latlonStep+",n="+pdata.length)
        }else{
          println("Process="+IATypes+";idName="+idName+";"+"FcTypeVariableName:"+pname)
        }
        (p.name, p)
      }
    )
    varHash
  }*/
  /*private def allVars2NameVar(IATypes:String,idName:Int,vals:ArrayBuffer[Variable],timeName:String,heightName:String,latName:String,lonName:String): ArrayBuffer[(String,Variable)] ={
    this.allVars2NameVar(IATypes,idName.toString,vals,timeName,heightName,latName,lonName)
  }
  def allVars2NameVar(IATypes:String, idName:String, vals:ArrayBuffer[Variable], timeName:String, heightName:String, latName:String, lonName:String): ArrayBuffer[(String,Variable)] ={
    val varHash = vals.map(
      p => {
        val pname=p.name
        val pdata=p.data()
        if(pname==timeName){
          print("timeName="+pname+":")
          for(i <- 0 to pdata.length-2){
            print(pdata(i)+",")
          }
          println(pdata(pdata.length-1))
        }else if(pname==heightName){
          print("heightName="+pname+":")
          for(i <- 0 to pdata.length-2){
            print(pdata(i)+",")
          }
          println(pdata(pdata.length-1))
        }else if(pname==latName || pname==lonName){
          val latlonStep:Double=DataFormatUtil.doubleFormat(pdata(1)-pdata(0))
          //          println("Process="+IATypes+";idName="+idName+";"+pname+ ".length=" + p.shape( )(1)+","+pname+"="+pdata(0)+"-"+pdata(pdata.length-1)+",step="+latlonStep+",n="+pdata.length)
        }else{
          println("Process="+IATypes+";idName="+idName+";"+"FcTypeVariableName:"+pname)
        }
        (p.name, p)
      }
    )
    varHash
  }*/

  //  def allVars2NameVar4(IATypes:String, idName:String, vals:ArrayBuffer[VariableCW], varsName:(String, String, String, String)): ArrayBuffer[(String,VariableCW)] ={
  //    val (timeName,heightName,latName,lonName)=varsName
  //    val varHash = vals.map(
  //      p => {
  //        /*val pname=p.name
  //        val pdata=p.data()
  //        if(pname==timeName){
  //          print("timeName="+pname+":")
  //          for(i <- 0 to pdata.length-2){
  //            print(pdata(i)+",")
  //          }
  //          println(pdata(pdata.length-1))
  //        }else if(pname==heightName){
  //          print("heightName="+pname+":")
  //          for(i <- 0 to pdata.length-2){
  //            print(pdata(i)+",")
  //          }
  //          println(pdata(pdata.length-1))
  //        }else if(pname==latName || pname==lonName){
  //          val latlonStep:Double=NumOperation.Kdec(pdata(1)-pdata(0))
  //          //          println("Process="+IATypes+";idName="+idName+";"+pname+ ".length=" + p.shape( )(1)+","+pname+"="+pdata(0)+"-"+pdata(pdata.length-1)+",step="+latlonStep+",n="+pdata.length)
  //        }else{
  //          println("Process="+IATypes+";idName="+idName+";"+"FcTypeVariableName:"+pname)
  //        }*/
  //        (p.name, p)
  //      }
  //    )
  //    varHash
  //  }
  //  def allVars2NameVar3(IATypes:String, idName:String, vals:ArrayBuffer[VariableCW], varsName:(String, String, String)): ArrayBuffer[(String,VariableCW)] ={
  //    val (timeName,latName,lonName)=varsName
  //    val varHash = vals.map(
  //      p => {
  //        /*val pname=p.name
  //        val pdata=p.data()
  //        if(pname==timeName){
  //          print("timeName="+pname+":")
  //          for(i <- 0 to pdata.length-2){
  //            print(pdata(i)+",")
  //          }
  //          println(pdata(pdata.length-1))
  //        }else if(pname==latName || pname==lonName){
  //          val latlonStep:Double=NumOperation.Kdec(pdata(1)-pdata(0))
  //          //          println("Process="+IATypes+";idName="+idName+";"+pname+ ".length=" + p.shape( )(1)+","+pname+"="+pdata(0)+"-"+pdata(pdata.length-1)+",step="+latlonStep+",n="+pdata.length)
  //        }else{
  //          println("Process="+IATypes+";idName="+idName+";"+"FcTypeVariableName:"+pname)
  //        }*/
  //        (p.name, p)
  //      }
  //    )
  //    varHash
  //  }

  def OneDimVar (Begin: String, End: String, Step: String, VarName_new: String, var_old: VariableCW): VariableCW = {
    val data: Array[Double] = ArrayOperation.ArithmeticArray(Begin.toDouble, End.toDouble, Step.toDouble)
    this.OneDimVar(data, VarName_new, var_old)
  }

  def OneDimVar (data: Array[Double], VarName_new: String, var_old: VariableCW): VariableCW = {
    val dims = List((VarName_new, data.length))
    new VariableCW(VarName_new, var_old.dataType, data, var_old.attributes, dims)
  }

  def empty4Var (VarName: String, dataLen: Int): VariableCW = {
    val data = Array.ofDim[Double](dataLen)
    val dims = List((VarName, dataLen))
    val attr = mutable.HashMap("unit" -> "1")
    new VariableCW(VarName, "double", data, attr, dims)
  }


  def creatVars4 (data: Array[Double], VarName: String, timeName: String, heightName: String, latName: String, lonName: String, timeLen: Int, heightLen: Int, latLen: Int, lonLen: Int): VariableCW = {
    val length = timeLen * heightLen * latLen * lonLen
    if (length == data.length) {
      val dims: List[(String, Int)] = List((timeName, timeLen), (heightName, heightLen), (latName, latLen), (lonName, lonLen))
      val attr: mutable.HashMap[String, String] = mutable.HashMap("unit" -> "1")
      new VariableCW(VarName, DataType.DOUBLE.toString, data, attr, dims)
    } else {
      val e: Exception = new Exception("creatVars4 配置参数不匹配,(timeLen*heightLen*latLen*lonLen=" + length + ")!=(data.length=" + data.length + ")")
      e.printStackTrace( )
      null
    }
  }

  def creatVars4 (data: Array[Float], VarName: String, timeName: String, heightName: String, latName: String, lonName: String, timeLen: Int, heightLen: Int, latLen: Int, lonLen: Int): VariableCW = {
    val length = timeLen * heightLen * latLen * lonLen
    if (length == data.length) {
      val dims: List[(String, Int)] = List((timeName, timeLen), (heightName, heightLen), (latName, latLen), (lonName, lonLen))
      val attr: mutable.HashMap[String, String] = mutable.HashMap("unit" -> "1")
      new VariableCW(VarName, DataType.FLOAT.toString, data, attr, dims)

    } else {
      val e: Exception = new Exception("creatVars4 配置参数不匹配,(timeLen*heightLen*latLen*lonLen=" + length + ")!=(data.length=" + data.length + ")")
      e.printStackTrace( )
      null
    }
  }

  def creatVars4 (data: Array[Long], VarName: String, timeName: String, heightName: String, latName: String, lonName: String, timeLen: Int, heightLen: Int, latLen: Int, lonLen: Int): VariableCW = {
    val length = timeLen * heightLen * latLen * lonLen
    if (length == data.length) {
      val dims: List[(String, Int)] = List((timeName, timeLen), (heightName, heightLen), (latName, latLen), (lonName, lonLen))
      val attr: mutable.HashMap[String, String] = mutable.HashMap("unit" -> "1")
      new VariableCW(VarName, DataType.LONG.toString, data, attr, dims)

    } else {
      val e: Exception = new Exception("creatVars4 配置参数不匹配,(timeLen*heightLen*latLen*lonLen=" + length + ")!=(data.length=" + data.length + ")")
      e.printStackTrace( )
      null
    }
  }

  def creatVars4 (data: Array[Short], VarName: String, timeName: String, heightName: String, latName: String, lonName: String, timeLen: Int, heightLen: Int, latLen: Int, lonLen: Int): VariableCW = {
    val length = timeLen * heightLen * latLen * lonLen
    if (length == data.length) {
      val dims: List[(String, Int)] = List((timeName, timeLen), (heightName, heightLen), (latName, latLen), (lonName, lonLen))
      val attr: mutable.HashMap[String, String] = mutable.HashMap("unit" -> "1")
      new VariableCW(VarName, DataType.SHORT.toString, data, attr, dims)

    } else {
      val e: Exception = new Exception("creatVars4 配置参数不匹配,(timeLen*heightLen*latLen*lonLen=" + length + ")!=(data.length=" + data.length + ")")
      e.printStackTrace( )
      null
    }
  }

  def creatVars4 (data: Array[Int], VarName: String, timeName: String, heightName: String, latName: String, lonName: String, timeLen: Int, heightLen: Int, latLen: Int, lonLen: Int): VariableCW = {
    val length = timeLen * heightLen * latLen * lonLen
    if (length == data.length) {
      val dims: List[(String, Int)] = List((timeName, timeLen), (heightName, heightLen), (latName, latLen), (lonName, lonLen))
      val attr: mutable.HashMap[String, String] = mutable.HashMap("unit" -> "1")
      new VariableCW(VarName, DataType.INT.toString, data, attr, dims)

    } else {
      val e: Exception = new Exception("creatVars4 配置参数不匹配,(timeLen*heightLen*latLen*lonLen=" + length + ")!=(data.length=" + data.length + ")")
      e.printStackTrace( )
      null
    }
  }

  def creatVars4 (data: Array[Byte], VarName: String, timeName: String, heightName: String, latName: String, lonName: String, timeLen: Int, heightLen: Int, latLen: Int, lonLen: Int): VariableCW = {
    val length = timeLen * heightLen * latLen * lonLen
    if (length == data.length) {
      val dims: List[(String, Int)] = List((timeName, timeLen), (heightName, heightLen), (latName, latLen), (lonName, lonLen))
      val attr: mutable.HashMap[String, String] = mutable.HashMap("unit" -> "1")
      new VariableCW(VarName, DataType.BYTE.toString, data, attr, dims)
    } else {
      val e: Exception = new Exception("creatVars4 配置参数不匹配,(timeLen*heightLen*latLen*lonLen=" + length + ")!=(data.length=" + data.length + ")")
      e.printStackTrace( )
      null
    }
  }

  def creatVars4 (data: Array[Double], oldVar: VariableCW): VariableCW = {
    new VariableCW(oldVar.name, data, oldVar.attributes, oldVar.dims)
  }

  def creatVars4 (data: ucar.ma2.Array, VarName: String, timeName: String, heightName: String, latName: String, lonName: String): VariableCW = {
    val attr: mutable.HashMap[String, String] = mutable.HashMap("unit" -> "1")
    this.creatVars4(data, attr, VarName, timeName, heightName, latName, lonName)
    //    if(data.isInstanceOf[ucar.ma2.Array]){
    //      val array=data.asInstanceOf[ucar.ma2.Array]
    //      val shape:Array[Int]=array.getShape
    //      val (timeLen,heightLen,latLen,lonLen)={
    //        (shape(0),shape(1),shape(2),shape(3))
    //      }
    //      val dims: List[(String, Int)] = List((timeName, timeLen), (heightName, heightLen), (latName, latLen), (lonName, lonLen))
    //      new VariableCW(VarName,array,attr,dims)
    //    }else{
    //      throw new Exception("data should be ucar.ma2.Array")
    //      null
    //    }
  }

  def creatVars4 (data: ucar.ma2.Array, attr: mutable.HashMap[String, String], VarName: String, timeName: String, heightName: String, latName: String, lonName: String): VariableCW = {
    //    val attr: mutable.HashMap[String, String] = mutable.HashMap("unit" -> "1")

    if (data.isInstanceOf[ucar.ma2.Array]) {
      val array = data.asInstanceOf[ucar.ma2.Array]
      val shape: Array[Int] = array.getShape
      val (timeLen, heightLen, latLen, lonLen) = {
        (shape(0), shape(1), shape(2), shape(3))
      }
      val dims: List[(String, Int)] = List((timeName, timeLen), (heightName, heightLen), (latName, latLen), (lonName, lonLen))
      new VariableCW(VarName, array, attr, dims)
    } else {
      throw new Exception("data should be ucar.ma2.Array")
      null
    }
  }

  def creatVars3 (data: ucar.ma2.Array, VarName: String, timeName: String, latName: String, lonName: String): VariableCW = {
    val attr: mutable.HashMap[String, String] = mutable.HashMap("unit" -> "1")
    this.creatVars3(data, attr, VarName, timeName, latName, lonName)
    //    if(data.isInstanceOf[ucar.ma2.Array]){
    //      val array=data.asInstanceOf[ucar.ma2.Array]
    //      val shape:Array[Int]=array.getShape
    //      val (timeLen,heightLen,latLen,lonLen)={
    //        (shape(0),shape(1),shape(2),shape(3))
    //      }
    //      val dims: List[(String, Int)] = List((timeName, timeLen), (heightName, heightLen), (latName, latLen), (lonName, lonLen))
    //      new VariableCW(VarName,array,attr,dims)
    //    }else{
    //      throw new Exception("data should be ucar.ma2.Array")
    //      null
    //    }
  }

  /*def creatVars4(data:ucar.ma2.Array,oldVar:VariableCW): VariableCW ={
    new VariableCW(oldVar.name,oldVar.dataType,data,oldVar.attributes,oldVar.dims)
  }*/
  def creatVars3 (data: ucar.ma2.Array, attr: mutable.HashMap[String, String], VarName: String, timeName: String, latName: String, lonName: String): VariableCW = {
    //    val attr: mutable.HashMap[String, String] = mutable.HashMap("unit" -> "1")

    if (data.isInstanceOf[ucar.ma2.Array]) {
      val array = data.asInstanceOf[ucar.ma2.Array]
      val shape: Array[Int] = array.getShape
      val (timeLen, latLen, lonLen) = {
        (shape(0), shape(1), shape(2))
      }
      val dims: List[(String, Int)] = List((timeName, timeLen), (latName, latLen), (lonName, lonLen))
      new VariableCW(VarName, array, attr, dims)
    } else {
      throw new Exception("data should be ucar.ma2.Array")
      null
    }
  }

  def creatVars3 (data: Array[Double], VarName: String, datatypeStr: String, timeName: String, latName: String, lonName: String, timeLen: Int, latLen: Int, lonLen: Int): VariableCW = {
    val length = timeLen * latLen * lonLen
    if (length == data.length) {
      val dims: List[(String, Int)] = List((timeName, timeLen), (latName, latLen), (lonName, lonLen))
      val shape: Array[Int] = Array(timeLen, latLen, lonLen)
      val attr: mutable.HashMap[String, String] = mutable.HashMap("unit" -> "1")
      new VariableCW(VarName, datatypeStr, data, attr, dims)
    } else {
      val e: Exception = new Exception("creatVars4 配置参数不匹配,(timeLen*heightLen*latLen*lonLen=" + length + ")!=(data.length=" + length + ")")
      e.printStackTrace( )
      null
    }
  }

  def creatVars2 (data: Array[Double], VarName: String, datatypeStr: String, latName: String, lonName: String, latLen: Int, lonLen: Int): VariableCW = {
    val length = latLen * lonLen
    if (length == data.length) {
      val dims: List[(String, Int)] = List((latName, latLen), (lonName, lonLen))
      val attr: mutable.HashMap[String, String] = mutable.HashMap("unit" -> "1")
      new VariableCW(VarName, datatypeStr, data, attr, dims)
    } else {
      val e: Exception = new Exception("creatVars4 配置参数不匹配,(timeLen*heightLen*latLen*lonLen=" + length + ")!=(data.length=" + length + ")")
      e.printStackTrace( )
      null
    }
  }

  def updataVars4 (data: Array[Double], oldVar: VariableCW): VariableCW = {
    val dims = oldVar.dims
    println(oldVar.name + ":updata Array,dataType=" + oldVar.dataType + ";dims=" + dims)
    new VariableCW(oldVar.name, oldVar.dataType, data, oldVar.attributes, dims)
  }

  def creatVars4 (data: ucar.ma2.Array, oldVar: VariableCW): VariableCW = {
    val dims0 = oldVar.dims
    val shape = data.getShape
    val dims: scala.List[(String, Int)] = {
      if (dims0.length == shape.length) {
        val dims1: ArrayBuffer[(String, Int)] = new ArrayBuffer[(String, Int)]( )
        var i = 0
        dims0.foreach(f => {
          val ele: (String, Int) = (f._1, shape(i))
          dims1.+=(ele)
          i += 1
        })
        dims1.toList
      } else {
        val e: Exception = new Exception("creatVars4参数出错")
        e.printStackTrace( )
        null
      }
    }
    println(oldVar.name + ":updata Array,dataType=" + oldVar.dataType + ";dims=" + dims)
    new VariableCW(oldVar.name, oldVar.dataType, data, oldVar.attributes, dims)
  }

  def creatVar (Begin: String, End: String, Step: String, VarName: String): VariableCW = {
    //    val data:Array[Double]=DataFormatUtil.doubleFormat2(AO.ArithmeticArray_FFF(Begin.toDouble,End.toDouble,Step.toDouble))
    this.creatVar(Begin.toDouble, End.toDouble, Step.toDouble, VarName)
  }

  def creatVar (Begin: Double, End: Double, Step: Double, VarName: String): VariableCW = {
    val data: Array[Double] = ArrayOperation.ArithmeticArray(Begin, End, Step)
    this.creatVar(data, VarName)
  }

  def creatVar (data: Array[Double], VarName: String): VariableCW = this.creatVar(data, VarName, "double")

  def creatVar (data: Array[Double], VarName: String, datatypeStr: String): VariableCW = {
    val dims = List((VarName, data.length))
    val attr = mutable.HashMap("unit" -> "1")
    new VariableCW(VarName, datatypeStr, data, attr, dims)
  }

  def creatVar (Begin: Double, End: Double, Step: Double, Var: VariableCW): VariableCW = {
    val data = ArrayOperation.ArithmeticArray(Begin, End, Step)
    this.creatVar(data, Var)
  }

  def creatVar(data: Array[Double], oldVar: VariableCW): VariableCW = {
    //    val formatdata=NumOperation.Kdec(data)
    val VarName = oldVar.name
    val dataType = oldVar.dataType
    val dims = List((VarName, data.length))
    new VariableCW(VarName, dataType, data, oldVar.attributes, dims)
  }

  def creatVar(data: Array[Float], oldVar: VariableCW): VariableCW = {
    //    val formatdata=NumOperation.Kdec(data)
    val VarName = oldVar.name
    val dataType = oldVar.dataType
    val dims = List((VarName, data.length))
    new VariableCW(VarName, dataType, data, oldVar.attributes, dims)
  }

  def creatVar (data: ucar.ma2.Array, VarName: String): VariableCW = {
    if (data.isInstanceOf[ucar.ma2.Array]) {
      val dims = List((VarName, data.getSize.toInt))
      val attr = mutable.HashMap("unit" -> "1")
      new VariableCW(VarName, data, attr, dims)
    } else {
      throw new Exception("data should be ucar.ma2.Array")
      null
    }
  }

//  def keepVar (rdd: RDD[SciDatasetCW], varsName: Array[String]): RDD[SciDatasetCW] = {
//    rdd.map(scidata => this.keepVar(scidata, varsName))
//  }

  def keepVar (scidata: SciDatasetCW, varsName: Array[String]): SciDatasetCW = {
    val VarsSet = scidata.variables.keySet
    VarsSet.foreach(f => {
      if (!varsName.contains(f)) {
        scidata.variables.remove(f)
      }
    })
    scidata
  }

  def Variable2Variable (variable: ucar.nc2.Variable): org.dia.core.Variable = {
    new org.dia.core.Variable(variable)
  }

  private def creatVars4 (data: Object, VarName: String, datatypeStr: String, timeName: String, heightName: String, latName: String, lonName: String, timeLen: Int, heightLen: Int, latLen: Int, lonLen: Int): VariableCW = {
    val attr: mutable.HashMap[String, String] = mutable.HashMap("unit" -> "1")
    val dims: List[(String, Int)] = List((timeName, timeLen), (heightName, heightLen), (latName, latLen), (lonName, lonLen))

    val length = timeLen * heightLen * latLen * lonLen
    DataType.getType(datatypeStr) match {
      case DataType.INT => new VariableCW(VarName, datatypeStr, data, attr, dims)
      case DataType.SHORT => new VariableCW(VarName, datatypeStr, data, attr, dims)
      case DataType.FLOAT => new VariableCW(VarName, datatypeStr, data, attr, dims)
      case DataType.DOUBLE => new VariableCW(VarName, datatypeStr, data, attr, dims)
      case DataType.LONG => new VariableCW(VarName, datatypeStr, data, attr, dims)
      case DataType.BYTE => new VariableCW(VarName, datatypeStr, data, attr, dims)
      case badType => {
        throw new Exception("Can't convert ma2.Array[" + badType + "] to numeric array.")
        null
      }
    }

  }


  //测试错误
  /*def Variable2Variable(variable: org.dia.core.Variable): ucar.nc2.Variable ={
    val dataType=DataType.getType(variable.dataType)
    val variableName=variable.name
    //(ncfile: NetcdfFile, group: Group, parent: Structure, shortName: String, dtype: DataType, dims: String)
    //(ncfile: NetcdfFile, group: Group, parent: Structure, shortName: String)

    val structure= new Structure(null, null, null, variableName)

    val varT=new ucar.nc2.Variable(null,null, structure,variableName)
    variable.attributes.foreach(p => {
      varT.addAttribute(new ucar.nc2.Attribute(p._1, p._2))
    })
    varT.setDataType(dataType)
    val variabledata: Object={
      if (dataType.equals(DataType.DOUBLE)) {
        variable.data( )
      } else if (dataType.equals(DataType.FLOAT)) {
        variable.data( ).map(d => d.toFloat)
      } else if (dataType.equals(DataType.INT)) {
        variable.data( ).map(d => {
          if(d.isNaN){
            Constant.intDefault
          }else{
            d.toInt
          }
        })
      } else if (dataType.equals(DataType.BYTE)) {
        variable.data( ).map(d => {
          if(d.isNaN){
            Constant.byteDefault
          }else{
            d.toByte
          }
        })
      } else if (dataType.equals(DataType.CHAR)) {
        variable.data( ).map(d => {
          if(d.isNaN){
            Constant.chartDefault
          }else{
            d.toChar
          }
        })
      } else if (dataType.equals(DataType.LONG)) {
        variable.data( ).map(d => {
          if(d.isNaN){
            Constant.longDefault
          }else{
            d.toLong
          }
        })
      } else if (dataType.equals(DataType.SHORT)) {
        variable.data( ).map(d => {
          if(d.isNaN){
            Constant.shortDefault
          }else{
            d.toShort
          }
        })
      } else {
        val e: Exception = new Exception("dataType=" + dataType + "没有设置该类型")
        variable.data( )
      }
    }
    val dataOut : ucar.ma2.Array= ucar.ma2.Array.factory(dataType, varT.getShape(), variabledata)
    varT.setCachedData(dataOut)
    varT
  }*/

}