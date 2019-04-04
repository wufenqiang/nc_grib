package com.weather.bigdata.it.nc_grib.core

import java.io.Serializable

import com.weather.bigdata.it.nc_grib.ReadWrite.nc_grib_PropertiesUtil
import org.dia.utils.NetCDFUtils
import ucar.ma2.{DataType, Index}
import ucar.nc2.{Attribute, Dimension}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class VariableCW (var name: String, var dataType:String, val array: ucar.ma2.Array, val shape:Array[Int], val attributes: mutable.HashMap[String, String], val dims: scala.List[(String, Int)])extends Serializable {
  nc_grib_PropertiesUtil.log.info("creat VariableCW:name=" + name + ",dataType=" + dataType + ",Len=" + shape.reduce((x, y) => x * y))
  def this(name: String, array: ucar.ma2.Array,attributes: mutable.HashMap[String, String], dims: scala.List[(String, Int)]){
    this(name: String,
      array.getElementType.toString.toUpperCase,
      array,
      array.getShape,
      attributes, {
        dims
      })
  }

  def this(name: String, nvar: ucar.nc2.Variable) {
    this(name,
      nvar.read()
      ,{
        val AttrMap=mutable.HashMap[String, String]()
        val nvarAttrs : java.util.List[Attribute]=nvar.getAttributes
        for(i<- 0 to nvarAttrs.size()-1){
          val attribute=nvarAttrs.get(i)
          val (k,v)=NetCDFUtils.convertAttribute(attribute)
          AttrMap.put(k,v)
        }
        AttrMap
      },{
        val dims:ArrayBuffer[(String, Int)]=new ArrayBuffer[(String, Int)]()
        val nvcardims: java.util.List[Dimension]=nvar.getDimensions
        for(i<- 0 to nvcardims.size()-1){
          val dim=nvcardims.get(i)
          val fullName=dim.getFullName
          val Length=dim.getLength
          dims.+=((fullName,Length))
        }
        dims.toList
      })

  }

  def this(nvar: ucar.nc2.Variable) {
    this(nvar.getFullName, nvar)
  }

  //新构造方法
  def this(name: String, array_Scala: Array[Double],attributes: mutable.HashMap[String, String],dims: scala.List[(String,Int)]){
    this(name: String, {
      val dataType = DataType.getType(array_Scala.head.getClass)
      val shape=dims.map(_._2).toArray
      val array : ucar.ma2.Array= ucar.ma2.Array.factory(dataType,shape,array_Scala)

      array
    }/*, dims.map(_._2).toArray*/,attributes: mutable.HashMap[String, String], dims: scala.List[(String, Int)])
  }
  def this(name: String, array_Scala: Array[Float],attributes: mutable.HashMap[String, String],dims: scala.List[(String,Int)]){
    this(name: String, {
      val dataType = DataType.getType(array_Scala.head.getClass)
      val shape=dims.map(_._2).toArray
      val array : ucar.ma2.Array= ucar.ma2.Array.factory(dataType,shape,array_Scala)
      array
    }/*, dims.map(_._2).toArray*/,attributes: mutable.HashMap[String, String], dims: scala.List[(String, Int)])
  }
  def this(name: String, array_Scala: Array[Byte],attributes: mutable.HashMap[String, String],dims: scala.List[(String,Int)]){
    this(name: String, {
      val dataType = DataType.getType(array_Scala.head.getClass)
      val shape=dims.map(_._2).toArray
      val array : ucar.ma2.Array= ucar.ma2.Array.factory(dataType,shape,array_Scala)
      array
    }/*, dims.map(_._2).toArray*/,attributes: mutable.HashMap[String, String], dims: scala.List[(String, Int)])
  }
  def this(name: String, array_Scala: Array[Long],attributes: mutable.HashMap[String, String],dims: scala.List[(String,Int)]){
    this(name: String, {
      val dataType = DataType.getType(array_Scala.head.getClass)
      val shape=dims.map(_._2).toArray
      val array : ucar.ma2.Array= ucar.ma2.Array.factory(dataType,shape,array_Scala)
      array
    }/*, dims.map(_._2).toArray*/,attributes: mutable.HashMap[String, String], dims: scala.List[(String, Int)])
  }
  def this(name: String, array_Scala: Array[Short],attributes: mutable.HashMap[String, String],dims: scala.List[(String,Int)]){
    this(name: String, {
      val dataType = DataType.getType(array_Scala.head.getClass)
      val shape=dims.map(_._2).toArray
      val array : ucar.ma2.Array= ucar.ma2.Array.factory(dataType,shape,array_Scala)
      array
    }/*, dims.map(_._2).toArray*/,attributes: mutable.HashMap[String, String], dims: scala.List[(String, Int)])
  }
  def this(name: String, array_Scala: Array[Int], attributes: mutable.HashMap[String, String],dims: scala.List[(String,Int)]){
    this(name: String, {
      val dataType = DataType.getType(array_Scala.head.getClass)
      val shape=dims.map(_._2).toArray
      val array : ucar.ma2.Array= ucar.ma2.Array.factory(dataType,shape,array_Scala)
      array
    }/*, dims.map(_._2).toArray*/,attributes: mutable.HashMap[String, String], dims: scala.List[(String, Int)])
  }


  def this(name: String, dataType: String,array_Scala0: Array[Double],attributes: mutable.HashMap[String, String],dims: scala.List[(String,Int)]){
    this(name: String, {
      val dataType0 : DataType= DataType.getType(dataType)
      val array_Scala={
        dataType0 match {
          case DataType.INT => array_Scala0.map(f=>f.toInt)
          case DataType.SHORT => array_Scala0.map(f=>f.toShort)
          case DataType.FLOAT => array_Scala0.map(f=>f.toFloat)
          case DataType.DOUBLE => array_Scala0
          case DataType.LONG => array_Scala0.map(f=>f.toLong)
          case DataType.BYTE => array_Scala0.map(f=>f.toByte)
          case badType => throw new Exception("Can't convert ma2.Array[" + badType + "] to numeric array.")
        }
      }
      val shape=dims.map(_._2).toArray
      val array : ucar.ma2.Array= ucar.ma2.Array.factory(dataType0,shape,array_Scala)
      array
    }/*, dims.map(_._2).toArray*/,attributes: mutable.HashMap[String, String], dims: scala.List[(String, Int)])
  }
  def this(name: String, dataType: String,array_Scala0: Array[Float], attributes: mutable.HashMap[String, String],dims: scala.List[(String,Int)]){
    this(name: String, {
      val dataType0 = DataType.getType(dataType)
      val array_Scala={
        dataType0 match {
          case DataType.INT => array_Scala0.map(f=>f.toInt)
          case DataType.SHORT => array_Scala0.map(f=>f.toShort)
          case DataType.FLOAT => array_Scala0
          case DataType.DOUBLE => array_Scala0.map(f=>f.toDouble)
          case DataType.LONG => array_Scala0.map(f=>f.toLong)
          case DataType.BYTE => array_Scala0.map(f=>f.toByte)
          case badType => throw new Exception("Can't convert ma2.Array[" + badType + "] to numeric array.")
        }
      }
      val shape=dims.map(_._2).toArray

      val array : ucar.ma2.Array= ucar.ma2.Array.factory(dataType0,shape,array_Scala)
      array
    }/*, dims.map(_._2).toArray*/,attributes: mutable.HashMap[String, String], dims: scala.List[(String, Int)])
  }
  def this(name: String, dataType: String,array_Scala0: Array[Byte], attributes: mutable.HashMap[String, String],dims: scala.List[(String,Int)]){
    this(name: String, {
      val dataType0 = DataType.getType(dataType)
      val array_Scala={
        dataType0 match {
          case DataType.INT => array_Scala0.map(f=>f.toInt)
          case DataType.SHORT => array_Scala0.map(f=>f.toShort)
          case DataType.FLOAT => array_Scala0.map(f=>f.toFloat)
          case DataType.DOUBLE => array_Scala0.map(f=>f.toDouble)
          case DataType.LONG => array_Scala0.map(f=>f.toLong)
          case DataType.BYTE => array_Scala0
          case badType => throw new Exception("Can't convert ma2.Array[" + badType + "] to numeric array.")
        }
      }
      val shape=dims.map(_._2).toArray
      val array : ucar.ma2.Array= ucar.ma2.Array.factory(dataType0,shape,array_Scala)
      array
    }/*, dims.map(_._2).toArray*/,attributes: mutable.HashMap[String, String], dims: scala.List[(String, Int)])
  }
  def this(name: String, dataType: String,array_Scala0: Array[Long], attributes: mutable.HashMap[String, String],dims: scala.List[(String,Int)]){
    this(name: String, {
      val dataType0 = DataType.getType(dataType)
      val array_Scala={
        dataType0 match {
          case DataType.INT => array_Scala0.map(f=>f.toInt)
          case DataType.SHORT => array_Scala0.map(f=>f.toShort)
          case DataType.FLOAT => array_Scala0.map(f=>f.toFloat)
          case DataType.DOUBLE => array_Scala0.map(f=>f.toDouble)
          case DataType.LONG => array_Scala0
          case DataType.BYTE => array_Scala0.map(f=>f.toByte)
          case badType => throw new Exception("Can't convert ma2.Array[" + badType + "] to numeric array.")
        }
      }
      val shape=dims.map(_._2).toArray
      val array : ucar.ma2.Array= ucar.ma2.Array.factory(dataType0,shape,array_Scala)
      array
    }/*, dims.map(_._2).toArray*/,attributes: mutable.HashMap[String, String], dims: scala.List[(String, Int)])
  }
  def this(name: String, dataType: String,array_Scala0: Array[Short], attributes: mutable.HashMap[String, String],dims: scala.List[(String,Int)]){
    this(name: String, {
      val dataType0 = DataType.getType(dataType)
      val array_Scala={
        dataType0 match {
          case DataType.INT => array_Scala0.map(f=>f.toInt)
          case DataType.SHORT => array_Scala0
          case DataType.FLOAT => array_Scala0.map(f=>f.toFloat)
          case DataType.DOUBLE => array_Scala0.map(f=>f.toDouble)
          case DataType.LONG => array_Scala0.map(f=>f.toLong)
          case DataType.BYTE => array_Scala0.map(f=>f.toByte)
          case badType => throw new Exception("Can't convert ma2.Array[" + badType + "] to numeric array.")
        }
      }
      val shape=dims.map(_._2).toArray
      val array : ucar.ma2.Array= ucar.ma2.Array.factory(dataType0,shape,array_Scala)
      array
    }/*, dims.map(_._2).toArray*/,attributes: mutable.HashMap[String, String], dims: scala.List[(String, Int)])
  }
  def this(name: String, dataType: String,array_Scala0: Array[Int], attributes: mutable.HashMap[String, String],dims: scala.List[(String,Int)]){
    this(name: String, {
      val dataType0 = DataType.getType(dataType)
      val array_Scala={
        dataType0 match {
          case DataType.INT => array_Scala0
          case DataType.SHORT => array_Scala0.map(f=>f.toShort)
          case DataType.FLOAT => array_Scala0.map(f=>f.toFloat)
          case DataType.DOUBLE => array_Scala0.map(f=>f.toDouble)
          case DataType.LONG => array_Scala0.map(f=>f.toLong)
          case DataType.BYTE => array_Scala0.map(f=>f.toByte)
          case badType => throw new Exception("Can't convert ma2.Array[" + badType + "] to numeric array.")
        }
      }
      val shape=dims.map(_._2).toArray
      val array : ucar.ma2.Array= ucar.ma2.Array.factory(dataType0,shape,array_Scala)
      array
    }/*, dims.map(_._2).toArray*/,attributes: mutable.HashMap[String, String], dims: scala.List[(String, Int)])
  }
  def this(name: String, dataType: String,array_Scala0: Object, attributes: mutable.HashMap[String, String],dims: scala.List[(String,Int)]){
    this(name: String, {
      /*val array:ucar.ma2.Array={
        if(array_Scala0.isInstanceOf[ucar.ma2.Array]){
          val array0=array_Scala0.asInstanceOf[ucar.ma2.Array]
          val arrayDataType:String=array0.getDataType.toString
          if(!dataType.equals(arrayDataType)){
            val e:Exception=new Exception("VariableCW输入数据类型矛盾dataType["+dataType+"]!=arrayDataType["+arrayDataType+"]")
            e.printStackTrace()
          }
          array0
        }else{
          val dataType0:DataType = DataType.getType(dataType)
          val array_Scala={
            dataType0 match {
              case DataType.INT => {
                if(array_Scala0.isInstanceOf[Array[Int]]){
                  array_Scala0.asInstanceOf[Array[Int]]
                }else if(array_Scala0.isInstanceOf[Array[Short]]){
                  array_Scala0.asInstanceOf[Array[Short]].map(f=>f.toInt)
                }else if(array_Scala0.isInstanceOf[Array[Float]]){
                  array_Scala0.asInstanceOf[Array[Float]].map(f=>f.toInt)
                }else if(array_Scala0.isInstanceOf[Array[Double]]){
                  array_Scala0.asInstanceOf[Array[Double]].map(f=>f.toInt)
                }else if(array_Scala0.isInstanceOf[Array[Long]]){
                  array_Scala0.asInstanceOf[Array[Long]].map(f=>f.toInt)
                }else if(array_Scala0.isInstanceOf[Array[Byte]]){
                  array_Scala0.asInstanceOf[Array[Byte]].map(f=>f.toInt)
                }else{
                  throw new Exception("Can't convert ma2.Array[" + dataType0 + "] to numeric array.")
                }
              }
              case DataType.SHORT =>{
                if(array_Scala0.isInstanceOf[Array[Int]]){
                  array_Scala0.asInstanceOf[Array[Int]].map(f=>f.toShort)
                }else if(array_Scala0.isInstanceOf[Array[Short]]){
                  array_Scala0.asInstanceOf[Array[Short]]
                }else if(array_Scala0.isInstanceOf[Array[Float]]){
                  array_Scala0.asInstanceOf[Array[Float]].map(f=>f.toShort)
                }else if(array_Scala0.isInstanceOf[Array[Double]]){
                  array_Scala0.asInstanceOf[Array[Double]].map(f=>f.toShort)
                }else if(array_Scala0.isInstanceOf[Array[Long]]){
                  array_Scala0.asInstanceOf[Array[Long]].map(f=>f.toShort)
                }else if(array_Scala0.isInstanceOf[Array[Byte]]){
                  array_Scala0.asInstanceOf[Array[Byte]].map(f=>f.toShort)
                }else{
                  throw new Exception("Can't convert ma2.Array[" + dataType0 + "] to numeric array.")
                }
              }
              case DataType.FLOAT => {
                if(array_Scala0.isInstanceOf[Array[Int]]){
                  array_Scala0.asInstanceOf[Array[Int]].map(f=>f.toFloat)
                }else if(array_Scala0.isInstanceOf[Array[Short]]){
                  array_Scala0.asInstanceOf[Array[Short]].map(f=>f.toFloat)
                }else if(array_Scala0.isInstanceOf[Array[Float]]){
                  array_Scala0.asInstanceOf[Array[Float]]
                }else if(array_Scala0.isInstanceOf[Array[Double]]){
                  array_Scala0.asInstanceOf[Array[Double]].map(f=>f.toFloat)
                }else if(array_Scala0.isInstanceOf[Array[Long]]){
                  array_Scala0.asInstanceOf[Array[Long]].map(f=>f.toFloat)
                }else if(array_Scala0.isInstanceOf[Array[Byte]]){
                  array_Scala0.asInstanceOf[Array[Byte]].map(f=>f.toFloat)
                }else{
                  throw new Exception("Can't convert ma2.Array[" + dataType0 + "] to numeric array.")
                }
              }
              case DataType.DOUBLE => {
                if(array_Scala0.isInstanceOf[Array[Int]]){
                  array_Scala0.asInstanceOf[Array[Int]].map(f=>f.toDouble)
                }else if(array_Scala0.isInstanceOf[Array[Short]]){
                  array_Scala0.asInstanceOf[Array[Short]].map(f=>f.toDouble)
                }else if(array_Scala0.isInstanceOf[Array[Float]]){
                  array_Scala0.asInstanceOf[Array[Float]].map(f=>f.toDouble)
                }else if(array_Scala0.isInstanceOf[Array[Double]]){
                  array_Scala0.asInstanceOf[Array[Double]]
                }else if(array_Scala0.isInstanceOf[Array[Long]]){
                  array_Scala0.asInstanceOf[Array[Long]].map(f=>f.toDouble)
                }else if(array_Scala0.isInstanceOf[Array[Byte]]){
                  array_Scala0.asInstanceOf[Array[Byte]].map(f=>f.toDouble)
                }else{
                  throw new Exception("Can't convert ma2.Array[" + dataType0 + "] to numeric array.")
                }
              }
              case DataType.LONG => {
                if(array_Scala0.isInstanceOf[Array[Int]]){
                  array_Scala0.asInstanceOf[Array[Int]].map(f=>f.toLong)
                }else if(array_Scala0.isInstanceOf[Array[Short]]){
                  array_Scala0.asInstanceOf[Array[Short]].map(f=>f.toLong)
                }else if(array_Scala0.isInstanceOf[Array[Float]]){
                  array_Scala0.asInstanceOf[Array[Float]].map(f=>f.toLong)
                }else if(array_Scala0.isInstanceOf[Array[Double]]){
                  array_Scala0.asInstanceOf[Array[Double]].map(f=>f.toLong)
                }else if(array_Scala0.isInstanceOf[Array[Long]]){
                  array_Scala0.asInstanceOf[Array[Long]]
                }else if(array_Scala0.isInstanceOf[Array[Byte]]){
                  array_Scala0.asInstanceOf[Array[Byte]].map(f=>f.toLong)
                }else{
                  throw new Exception("Can't convert ma2.Array[" + dataType0 + "] to numeric array.")
                }
              }
              case DataType.BYTE => {
                if(array_Scala0.isInstanceOf[Array[Int]]){
                  array_Scala0.asInstanceOf[Array[Int]].map(f=>f.toByte)
                }else if(array_Scala0.isInstanceOf[Array[Short]]){
                  array_Scala0.asInstanceOf[Array[Short]].map(f=>f.toByte)
                }else if(array_Scala0.isInstanceOf[Array[Float]]){
                  array_Scala0.asInstanceOf[Array[Float]].map(f=>f.toByte)
                }else if(array_Scala0.isInstanceOf[Array[Double]]){
                  array_Scala0.asInstanceOf[Array[Double]].map(f=>f.toByte)
                }else if(array_Scala0.isInstanceOf[Array[Long]]){
                  array_Scala0.asInstanceOf[Array[Long]].map(f=>f.toByte)
                }else if(array_Scala0.isInstanceOf[Array[Byte]]){
                  array_Scala0.asInstanceOf[Array[Byte]]
                }else{
                  throw new Exception("Can't convert ma2.Array[" + dataType0 + "] to numeric array.")
                }
              }
              case badType => throw new Exception("Can't convert ma2.Array[" + badType + "] to numeric array.")
            }
          }
          val shape=dims.map(_._2).toArray
          ucar.ma2.Array.factory(dataType0,shape,array_Scala)
        }
      }
      array*/
      if(array_Scala0.isInstanceOf[ucar.ma2.Array]){
        val array_Scala1=array_Scala0.asInstanceOf[ucar.ma2.Array]
        /*val dataType0Str:String=array_Scala1.getDataType.toString
        if(dataType0Str.equals(dataType)){
          array_Scala1
        }else{
          val dataType0=DataType.getType(dataType)
          val dataType1=array_Scala1.getDataType
          val shape1:Array[Int]=array_Scala1.getShape
          val len1:Int=shape1.reduce((x,y)=>x*y)
          val array0= ucar.ma2.Array.factory(dataType0,shape1)
          dataType1 match {
            case DataType.INT =>{
              dataType0 match {
                case DataType.SHORT =>{
                    for(i<- 0 to len1-1){
                      ucarma2ArrayOperationCW.setObject(array0,i,array_Scala1.getShort(i))
                    }

                }
                case DataType.FLOAT =>{

                }
                case DataType.DOUBLE =>{

                }
                case DataType.LONG =>{

                }
                case DataType.BYTE =>
                case badType => throw new Exception("Can't convert ma2.Array[" + badType + "] to numeric array.")
              }
            }
            case DataType.SHORT =>
            case DataType.FLOAT =>
            case DataType.DOUBLE =>
            case DataType.LONG =>
            case DataType.BYTE =>
            case badType => throw new Exception("Can't convert ma2.Array[" + badType + "] to numeric array.")
          }
          array0
        }*/

        //        ucarma2ArrayOperationCW.setObjects(array_Scala1,dataType)
        array_Scala1
      }else{
        val dataType0:DataType = DataType.getType(dataType)
        val array_Scala={
          dataType0 match {
            case DataType.INT => {
              if(array_Scala0.isInstanceOf[Array[Int]]){
                array_Scala0.asInstanceOf[Array[Int]]
              }else if(array_Scala0.isInstanceOf[Array[Short]]){
                array_Scala0.asInstanceOf[Array[Short]].map(f=>f.toInt)
              }else if(array_Scala0.isInstanceOf[Array[Float]]){
                array_Scala0.asInstanceOf[Array[Float]].map(f=>f.toInt)
              }else if(array_Scala0.isInstanceOf[Array[Double]]){
                array_Scala0.asInstanceOf[Array[Double]].map(f=>f.toInt)
              }else if(array_Scala0.isInstanceOf[Array[Long]]){
                array_Scala0.asInstanceOf[Array[Long]].map(f=>f.toInt)
              }else if(array_Scala0.isInstanceOf[Array[Byte]]){
                array_Scala0.asInstanceOf[Array[Byte]].map(f=>f.toInt)
              }else{
                throw new Exception("Can't convert ma2.Array[" + dataType0 + "] to numeric array.")
              }
            }
            case DataType.SHORT =>{
              if(array_Scala0.isInstanceOf[Array[Int]]){
                array_Scala0.asInstanceOf[Array[Int]].map(f=>f.toShort)
              }else if(array_Scala0.isInstanceOf[Array[Short]]){
                array_Scala0.asInstanceOf[Array[Short]]
              }else if(array_Scala0.isInstanceOf[Array[Float]]){
                array_Scala0.asInstanceOf[Array[Float]].map(f=>f.toShort)
              }else if(array_Scala0.isInstanceOf[Array[Double]]){
                array_Scala0.asInstanceOf[Array[Double]].map(f=>f.toShort)
              }else if(array_Scala0.isInstanceOf[Array[Long]]){
                array_Scala0.asInstanceOf[Array[Long]].map(f=>f.toShort)
              }else if(array_Scala0.isInstanceOf[Array[Byte]]){
                array_Scala0.asInstanceOf[Array[Byte]].map(f=>f.toShort)
              }else{
                throw new Exception("Can't convert ma2.Array[" + dataType0 + "] to numeric array.")
              }
            }
            case DataType.FLOAT => {
              if(array_Scala0.isInstanceOf[Array[Int]]){
                array_Scala0.asInstanceOf[Array[Int]].map(f=>f.toFloat)
              }else if(array_Scala0.isInstanceOf[Array[Short]]){
                array_Scala0.asInstanceOf[Array[Short]].map(f=>f.toFloat)
              }else if(array_Scala0.isInstanceOf[Array[Float]]){
                array_Scala0.asInstanceOf[Array[Float]]
              }else if(array_Scala0.isInstanceOf[Array[Double]]){
                array_Scala0.asInstanceOf[Array[Double]].map(f=>f.toFloat)
              }else if(array_Scala0.isInstanceOf[Array[Long]]){
                array_Scala0.asInstanceOf[Array[Long]].map(f=>f.toFloat)
              }else if(array_Scala0.isInstanceOf[Array[Byte]]){
                array_Scala0.asInstanceOf[Array[Byte]].map(f=>f.toFloat)
              }else{
                throw new Exception("Can't convert ma2.Array[" + dataType0 + "] to numeric array.")
              }
            }
            case DataType.DOUBLE => {
              if(array_Scala0.isInstanceOf[Array[Int]]){
                array_Scala0.asInstanceOf[Array[Int]].map(f=>f.toDouble)
              }else if(array_Scala0.isInstanceOf[Array[Short]]){
                array_Scala0.asInstanceOf[Array[Short]].map(f=>f.toDouble)
              }else if(array_Scala0.isInstanceOf[Array[Float]]){
                array_Scala0.asInstanceOf[Array[Float]].map(f=>f.toDouble)
              }else if(array_Scala0.isInstanceOf[Array[Double]]){
                array_Scala0.asInstanceOf[Array[Double]]
              }else if(array_Scala0.isInstanceOf[Array[Long]]){
                array_Scala0.asInstanceOf[Array[Long]].map(f=>f.toDouble)
              }else if(array_Scala0.isInstanceOf[Array[Byte]]){
                array_Scala0.asInstanceOf[Array[Byte]].map(f=>f.toDouble)
              }else{
                throw new Exception("Can't convert ma2.Array[" + dataType0 + "] to numeric array.")
              }
            }
            case DataType.LONG => {
              if(array_Scala0.isInstanceOf[Array[Int]]){
                array_Scala0.asInstanceOf[Array[Int]].map(f=>f.toLong)
              }else if(array_Scala0.isInstanceOf[Array[Short]]){
                array_Scala0.asInstanceOf[Array[Short]].map(f=>f.toLong)
              }else if(array_Scala0.isInstanceOf[Array[Float]]){
                array_Scala0.asInstanceOf[Array[Float]].map(f=>f.toLong)
              }else if(array_Scala0.isInstanceOf[Array[Double]]){
                array_Scala0.asInstanceOf[Array[Double]].map(f=>f.toLong)
              }else if(array_Scala0.isInstanceOf[Array[Long]]){
                array_Scala0.asInstanceOf[Array[Long]]
              }else if(array_Scala0.isInstanceOf[Array[Byte]]){
                array_Scala0.asInstanceOf[Array[Byte]].map(f=>f.toLong)
              }else{
                throw new Exception("Can't convert ma2.Array[" + dataType0 + "] to numeric array.")
              }
            }
            case DataType.BYTE => {
              if(array_Scala0.isInstanceOf[Array[Int]]){
                array_Scala0.asInstanceOf[Array[Int]].map(f=>f.toByte)
              }else if(array_Scala0.isInstanceOf[Array[Short]]){
                array_Scala0.asInstanceOf[Array[Short]].map(f=>f.toByte)
              }else if(array_Scala0.isInstanceOf[Array[Float]]){
                array_Scala0.asInstanceOf[Array[Float]].map(f=>f.toByte)
              }else if(array_Scala0.isInstanceOf[Array[Double]]){
                array_Scala0.asInstanceOf[Array[Double]].map(f=>f.toByte)
              }else if(array_Scala0.isInstanceOf[Array[Long]]){
                array_Scala0.asInstanceOf[Array[Long]].map(f=>f.toByte)
              }else if(array_Scala0.isInstanceOf[Array[Byte]]){
                array_Scala0.asInstanceOf[Array[Byte]]
              }else{
                throw new Exception("Can't convert ma2.Array[" + dataType0 + "] to numeric array.")
              }
            }
            case badType => throw new Exception("Can't convert ma2.Array[" + badType + "] to numeric array.")
          }
        }
        val shape=dims.map(_._2).toArray
        ucar.ma2.Array.factory(dataType0,shape,array_Scala)
      }
    }/*, dims.map(_._2).toArray*/,attributes: mutable.HashMap[String, String], dims: scala.List[(String, Int)])
  }
  //  def this(name: String, array: ucar.ma2.Array, dims:mutable.HashMap[String, Int]) {
  //    this(name,array,array.getShape,new mutable.HashMap[String, String](),dims)
  //  }


  def insertAttributes(metaDataVar: (String, String)*): Unit = {
    for (variable <- metaDataVar) attributes += variable
  }

  def insertAttributes(metaDataVars: TraversableOnce[(String, String)]): Unit = {
    attributes ++= metaDataVars
  }



  /**
    * Sets the variable name and returns the Variable
    * @param name new name to set
    * @return
    */
  def setName(name : String) : VariableCW = {
    this.name = name

    def Len:Int=shape.reduce((x,y)=>(x*y))
    println("reSet VariableCW:name="+name+",dataType="+dataType+",Len="+Len)

    this
  }
  def setDataType(dataType0:DataType): Unit ={
    this.dataType=dataType0.toString
  }
  def setDataType(dataType0Str:String):Unit={
    this.setDataType(DataType.getType(dataType0Str))
  }

  /**
    * Returns the array corresponding to the variable in use.
    * This is to mimic the numpy like syntax of var[:]
    * Example usage: val absT = var()
    *
    * @return ucar.ma2.Array corresponding to variable in use
    */
  def apply(): ucar.ma2.Array = this.array

  def update(key: String, attribute: String): VariableCW = {
    attributes(key) = attribute
    this
  }

  def getsize(): Int = this.getLen()

  def length(): Int = this.getLen()

  //  def shape(): Array[Int] = array.getShape
  def getLen(): Int = this.shape.reduce((x, y) => x * y)


  def dataDouble(): Array[Double] = {
    this.dataDouble1()
  }

  def dataFloat(): Array[Float] = {
    this.dataFloat1()
  }

  def dataInt(): Array[Int] = {
    this.dataInt1()
  }
  private def dataDouble0():Array[Double]= {
    val arrayObject:Object=array.get1DJavaArray(array.getElementType)
    DataType.getType(dataType) match {
      case DataType.INT => arrayObject.asInstanceOf[Array[Int]].map(_.toDouble)
      case DataType.SHORT => arrayObject.asInstanceOf[Array[Short]].map(_.toDouble)
      case DataType.FLOAT => arrayObject.asInstanceOf[Array[Float]].map(_.toDouble)
      case DataType.DOUBLE => arrayObject.asInstanceOf[Array[Double]]
      case DataType.LONG => arrayObject.asInstanceOf[Array[Long]].map(_.toDouble)
      case DataType.BYTE => arrayObject.asInstanceOf[Array[Byte]].map(_.toDouble)
      case badType => {
        throw new Exception("dataDouble0 Can't convert ma2.Array[" + badType + "] to numeric array["+badType+"]")
        null
      }
    }
  }
  private def dataDouble1():Array[Double]= {
    DataType.getType(dataType) match {
      case DataType.INT => {
        Array.range(0,getLen()).map(index=>array.getInt(index).toDouble)
      }
      case DataType.SHORT => {
        Array.range(0,getLen()).map(index=>array.getShort(index).toDouble)
      }
      case DataType.FLOAT => {
        Array.range(0,getLen()).map(index=>array.getFloat(index).toDouble)
      }
      case DataType.DOUBLE => {
        Array.range(0,getLen()).map(index=>array.getDouble(index))
      }
      case DataType.LONG => {
        Array.range(0,getLen()).map(index=>array.getLong(index).toDouble)
      }
      case DataType.BYTE => {
        Array.range(0,getLen()).map(index=>array.getByte(index).toDouble)
      }
      case badType => {
        throw new Exception("dataDouble1 Can't convert ma2.Array[" + badType + "] to numeric array["+badType+"]")
        null
      }
    }
  }
  private def dataFloat1():Array[Float]= {
    DataType.getType(dataType) match {
      case DataType.INT => {
        Array.range(0,getLen()).map(index=>array.getInt(index).toFloat)
      }
      case DataType.SHORT => {
        Array.range(0,getLen()).map(index=>array.getShort(index).toFloat)
      }
      case DataType.FLOAT => {
        Array.range(0,getLen()).map(index=>array.getFloat(index))
      }
      case DataType.DOUBLE => {
        Array.range(0,getLen()).map(index=>array.getDouble(index).toFloat)
      }
      case DataType.LONG => {
        Array.range(0,getLen()).map(index=>array.getLong(index).toFloat)
      }
      case DataType.BYTE => {
        Array.range(0,getLen()).map(index=>array.getByte(index).toFloat)
      }
      case badType => {
        throw new Exception("dataFloat1 Can't convert ma2.Array[" + badType + "] to numeric array["+badType+"]")
        null
      }
    }
  }
  private def dataInt1(): Array[Int] = {
    DataType.getType(dataType) match {
      case DataType.INT => {
        Array.range(0, getLen()).map(index => array.getInt(index))
      }
      case DataType.SHORT => {
        Array.range(0, getLen()).map(index => array.getShort(index).toInt)
      }
      case DataType.FLOAT => {
        Array.range(0, getLen()).map(index => array.getFloat(index).toInt)
      }
      case DataType.DOUBLE => {
        Array.range(0, getLen()).map(index => array.getDouble(index).toInt)
      }
      case DataType.LONG => {
        Array.range(0, getLen()).map(index => array.getLong(index).toInt)
      }
      case DataType.BYTE => {
        Array.range(0, getLen()).map(index => array.getByte(index).toInt)
      }
      case badType => {
        throw new Exception("dataInt1 Can't convert ma2.Array[" + badType + "] to numeric array["+badType+"]")
        null
      }
    }
  }


  private val arraydataType0=array.getDataType
  def dataDouble(index:Int):Double={
    this.arraydataType0 match {
      case DataType.INT => array.getInt(index).toDouble
      case DataType.SHORT => array.getShort(index).toDouble
      case DataType.FLOAT => array.getFloat(index).toDouble
      case DataType.DOUBLE => array.getDouble(index)
      case DataType.LONG => array.getLong(index).toDouble
      case DataType.BYTE => array.getByte(index).toDouble
      case badType => {
        throw new Exception("Can't convert ma2.Array[" + badType + "] to numeric array.")
      }
    }
  }
  def dataFloat(index:Int):Float={
    this.arraydataType0 match {
      case DataType.INT => array.getInt(index).toFloat
      case DataType.SHORT => array.getShort(index).toFloat
      case DataType.FLOAT => array.getFloat(index)
      case DataType.DOUBLE => array.getDouble(index).toFloat
      case DataType.LONG => array.getLong(index).toFloat
      case DataType.BYTE => array.getByte(index).toFloat
      case badType => {
        throw new Exception("Can't convert ma2.Array[" + badType + "] to numeric array.")
      }
    }
  }

  def dataInt(index: Int): Int = {
    this.arraydataType0 match {
      case DataType.INT => array.getInt(index)
      case DataType.SHORT => array.getShort(index).toInt
      case DataType.FLOAT => array.getFloat(index).toInt
      case DataType.DOUBLE => array.getDouble(index).toInt
      case DataType.LONG => array.getLong(index).toInt
      case DataType.BYTE => array.getByte(index).toInt
      case badType => {
        throw new Exception("Can't convert ma2.Array[" + badType + "] to numeric array.")
      }
    }
  }
  private def dataByte(index:Int):Byte={
    this.arraydataType0 match {
      case DataType.INT => array.getInt(index).toByte
      case DataType.SHORT => array.getShort(index).toByte
      case DataType.FLOAT => array.getFloat(index).toByte
      case DataType.DOUBLE => array.getDouble(index).toByte
      case DataType.LONG => array.getLong(index).toByte
      case DataType.BYTE => array.getByte(index)
      case badType => {
        throw new Exception("Can't convert ma2.Array[" + badType + "] to numeric array.")
      }
    }
  }
  private def dataLong(index:Int):Long={
    this.arraydataType0 match {
      case DataType.INT => array.getInt(index).toLong
      case DataType.SHORT => array.getShort(index).toLong
      case DataType.FLOAT => array.getFloat(index).toLong
      case DataType.DOUBLE => array.getDouble(index).toLong
      case DataType.LONG => array.getLong(index)
      case DataType.BYTE => array.getByte(index).toLong
      case badType => {
        throw new Exception("Can't convert ma2.Array[" + badType + "] to numeric array.")
      }
    }
  }
  private def dataShort(index:Int):Short={
    this.arraydataType0 match {
      case DataType.INT => array.getInt(index).toShort
      case DataType.SHORT => array.getShort(index)
      case DataType.FLOAT => array.getFloat(index).toShort
      case DataType.DOUBLE => array.getDouble(index).toShort
      case DataType.LONG => array.getLong(index).toShort
      case DataType.BYTE => array.getByte(index).toShort
      case badType => {
        throw new Exception("Can't convert ma2.Array[" + badType + "] to numeric array.")
      }
    }
  }
  def dataObject(index:Int):AnyVal={
    this.arraydataType0 match {
      case DataType.INT => this.dataInt(index)
      case DataType.SHORT => this.dataShort(index)
      case DataType.FLOAT => this.dataFloat(index)
      case DataType.DOUBLE => this.dataDouble(index)
      case DataType.LONG => this.dataLong(index)
      case DataType.BYTE => this.dataByte(index)
      case badType => {
        throw new Exception("Can't convert ma2.Array[" + badType + "] to numeric array.")
      }
    }
  }

  private def setDoubledata(index:Int,v:Double): Unit ={
    this.array.setDouble(index,v)
  }
  private def setFloatdata(index:Int,v:Float): Unit ={
    this.array.setFloat(index,v)
  }
  private def setBytedata(index:Int,v:Byte): Unit ={
    this.array.setByte(index,v)
  }
  private def setIntdata(index:Int,v:Int): Unit ={
    this.array.setInt(index,v)
  }
  private def setLongdata(index:Int,v:Long): Unit ={
    this.array.setLong(index,v)
  }
  private def setShortdata(index:Int,v:Short): Unit ={
    this.array.setShort(index,v)
  }

  private def setDoubledata(index:Index,v:Double): Unit ={
    this.array.setDouble(index,v)
  }
  private def setFloatdata(index:Index,v:Float): Unit ={
    this.array.setFloat(index,v)
  }
  private def setBytedata(index:Index,v:Byte): Unit ={
    this.array.setByte(index,v)
  }
  private def setIntdata(index:Index,v:Int): Unit ={
    this.array.setInt(index,v)
  }
  private def setLongdata(index:Index,v:Long): Unit ={
    this.array.setLong(index,v)
  }
  private def setShortdata(index:Index,v:Short): Unit ={
    this.array.setShort(index,v)
  }

  def setdata(index:Int,v:Double): Unit ={
    DataType.getType(dataType) match {
      case DataType.INT => this.setIntdata(index,v.toInt)
      case DataType.SHORT =>  this.setShortdata(index,v.toShort)
      case DataType.FLOAT =>  this.setFloatdata(index,v.toFloat)
      case DataType.DOUBLE =>  this.setDoubledata(index,v)
      case DataType.LONG => this.setLongdata(index,v.toLong)
      case DataType.BYTE =>  this.setBytedata(index,v.toByte)
      case badType => {
        throw new Exception("Can't convert ma2.Array[" + badType + "] to numeric array.")
      }
    }
  }
  def setdata(index:Int,v:Float): Unit ={
    DataType.getType(dataType) match {
      case DataType.INT => this.setIntdata(index,v.toInt)
      case DataType.SHORT =>  this.setShortdata(index,v.toShort)
      case DataType.FLOAT =>  this.setFloatdata(index,v)
      case DataType.DOUBLE =>  this.setDoubledata(index,v.toDouble)
      case DataType.LONG => this.setLongdata(index,v.toLong)
      case DataType.BYTE =>  this.setBytedata(index,v.toByte)
      case badType => {
        throw new Exception("Can't convert ma2.Array[" + badType + "] to numeric array.")
      }
    }
  }
  def setdata(index:Int,v:Byte): Unit ={
    DataType.getType(dataType) match {
      case DataType.INT => this.setIntdata(index,v.toInt)
      case DataType.SHORT =>  this.setShortdata(index,v.toShort)
      case DataType.FLOAT =>  this.setFloatdata(index,v.toFloat)
      case DataType.DOUBLE =>  this.setDoubledata(index,v.toDouble)
      case DataType.LONG => this.setLongdata(index,v.toLong)
      case DataType.BYTE =>  this.setBytedata(index,v)
      case badType => {
        throw new Exception("Can't convert ma2.Array[" + badType + "] to numeric array.")
      }
    }
  }
  def setdata(index:Int,v:Int): Unit ={
    DataType.getType(dataType) match {
      case DataType.INT => this.setIntdata(index,v)
      case DataType.SHORT =>  this.setShortdata(index,v.toShort)
      case DataType.FLOAT =>  this.setFloatdata(index,v.toFloat)
      case DataType.DOUBLE =>  this.setDoubledata(index,v.toDouble)
      case DataType.LONG => this.setLongdata(index,v.toLong)
      case DataType.BYTE =>  this.setBytedata(index,v.toByte)
      case badType => {
        throw new Exception("Can't convert ma2.Array[" + badType + "] to numeric array.")
      }
    }
  }
  def setdata(index:Int,v:Long): Unit ={
    DataType.getType(dataType) match {
      case DataType.INT => this.setIntdata(index,v.toInt)
      case DataType.SHORT =>  this.setShortdata(index,v.toShort)
      case DataType.FLOAT =>  this.setFloatdata(index,v.toFloat)
      case DataType.DOUBLE =>  this.setDoubledata(index,v.toDouble)
      case DataType.LONG => this.setLongdata(index,v)
      case DataType.BYTE =>  this.setBytedata(index,v.toByte)
      case badType => {
        throw new Exception("Can't convert ma2.Array[" + badType + "] to numeric array.")
      }
    }
  }
  def setdata(index:Int,v:Short): Unit ={
    DataType.getType(dataType) match {
      case DataType.INT => this.setIntdata(index,v.toInt)
      case DataType.SHORT =>  this.setShortdata(index,v)
      case DataType.FLOAT =>  this.setFloatdata(index,v.toFloat)
      case DataType.DOUBLE =>  this.setDoubledata(index,v.toDouble)
      case DataType.LONG => this.setLongdata(index,v.toLong)
      case DataType.BYTE =>  this.setBytedata(index,v.toByte)
      case badType => {
        throw new Exception("Can't convert ma2.Array[" + badType + "] to numeric array.")
      }
    }
  }

  def setdata(index:Index,v:Double): Unit ={
    DataType.getType(dataType) match {
      case DataType.INT => this.setIntdata(index,v.toInt)
      case DataType.SHORT =>  this.setShortdata(index,v.toShort)
      case DataType.FLOAT =>  this.setFloatdata(index,v.toFloat)
      case DataType.DOUBLE =>  this.setDoubledata(index,v)
      case DataType.LONG => this.setLongdata(index,v.toLong)
      case DataType.BYTE =>  this.setBytedata(index,v.toByte)
      case badType => {
        throw new Exception("Can't convert ma2.Array[" + badType + "] to numeric array.")
      }
    }
  }
  def setdata(index:Index,v:Float): Unit ={
    DataType.getType(dataType) match {
      case DataType.INT => this.setIntdata(index,v.toInt)
      case DataType.SHORT =>  this.setShortdata(index,v.toShort)
      case DataType.FLOAT =>  this.setFloatdata(index,v)
      case DataType.DOUBLE =>  this.setDoubledata(index,v.toDouble)
      case DataType.LONG => this.setLongdata(index,v.toLong)
      case DataType.BYTE =>  this.setBytedata(index,v.toByte)
      case badType => {
        throw new Exception("Can't convert ma2.Array[" + badType + "] to numeric array.")
      }
    }
  }
  def setdata(index:Index,v:Byte): Unit ={
    DataType.getType(dataType) match {
      case DataType.INT => this.setIntdata(index,v.toInt)
      case DataType.SHORT =>  this.setShortdata(index,v.toShort)
      case DataType.FLOAT =>  this.setFloatdata(index,v.toFloat)
      case DataType.DOUBLE =>  this.setDoubledata(index,v.toDouble)
      case DataType.LONG => this.setLongdata(index,v.toLong)
      case DataType.BYTE =>  this.setBytedata(index,v)
      case badType => {
        throw new Exception("Can't convert ma2.Array[" + badType + "] to numeric array.")
      }
    }
  }
  def setdata(index:Index,v:Int): Unit ={
    DataType.getType(dataType) match {
      case DataType.INT => this.setIntdata(index,v)
      case DataType.SHORT =>  this.setShortdata(index,v.toShort)
      case DataType.FLOAT =>  this.setFloatdata(index,v.toFloat)
      case DataType.DOUBLE =>  this.setDoubledata(index,v.toDouble)
      case DataType.LONG => this.setLongdata(index,v.toLong)
      case DataType.BYTE =>  this.setBytedata(index,v.toByte)
      case badType => {
        throw new Exception("Can't convert ma2.Array[" + badType + "] to numeric array.")
      }
    }
  }
  def setdata(index:Index,v:Long): Unit ={
    DataType.getType(dataType) match {
      case DataType.INT => this.setIntdata(index,v.toInt)
      case DataType.SHORT =>  this.setShortdata(index,v.toShort)
      case DataType.FLOAT =>  this.setFloatdata(index,v.toFloat)
      case DataType.DOUBLE =>  this.setDoubledata(index,v.toDouble)
      case DataType.LONG => this.setLongdata(index,v)
      case DataType.BYTE =>  this.setBytedata(index,v.toByte)
      case badType => {
        throw new Exception("Can't convert ma2.Array[" + badType + "] to numeric array.")
      }
    }
  }
  def setdata(index:Index,v:Short): Unit ={
    DataType.getType(dataType) match {
      case DataType.INT => this.setIntdata(index,v.toInt)
      case DataType.SHORT =>  this.setShortdata(index,v)
      case DataType.FLOAT =>  this.setFloatdata(index,v.toFloat)
      case DataType.DOUBLE =>  this.setDoubledata(index,v.toDouble)
      case DataType.LONG => this.setLongdata(index,v.toLong)
      case DataType.BYTE =>  this.setBytedata(index,v.toByte)
      case badType => {
        throw new Exception("Can't convert ma2.Array[" + badType + "] to numeric array.")
      }
    }
  }

  def setdata(index:Int,v:Object): Unit ={
    DataType.getType(dataType) match {
      case DataType.INT => this.setdata(index,v.asInstanceOf[Int])
      case DataType.SHORT =>  this.setdata(index,v.asInstanceOf[Short])
      case DataType.FLOAT =>  this.setdata(index,v.asInstanceOf[Float])
      case DataType.DOUBLE =>  this.setdata(index,v.asInstanceOf[Double])
      case DataType.LONG => this.setdata(index,v.asInstanceOf[Long])
      case DataType.BYTE =>  this.setdata(index,v.asInstanceOf[Byte])
      case badType => {
        throw new Exception("Can't convert ma2.Array[" + badType + "] to numeric array.")
      }
    }
  }
  def setdata(index:Index,v:Object): Unit ={
    DataType.getType(dataType) match {
      case DataType.INT => this.setdata(index,v.asInstanceOf[Int])
      case DataType.SHORT =>  this.setdata(index,v.asInstanceOf[Short])
      case DataType.FLOAT =>  this.setdata(index,v.asInstanceOf[Float])
      case DataType.DOUBLE =>  this.setdata(index,v.asInstanceOf[Double])
      case DataType.LONG => this.setdata(index,v.asInstanceOf[Long])
      case DataType.BYTE =>  this.setdata(index,v.asInstanceOf[Byte])
      case badType => {
        throw new Exception("Can't convert ma2.Array[" + badType + "] to numeric array.")
      }
    }
  }

  //  def setdatas(indexs:Array[Int],vs:Array[Double]): Unit ={
  //    val len=indexs.length
  //    if(len==vs.length){
  //      for(i<- 0 to len-1){
  //        this.setdata(i,vs(i))
  //      }
  //    }
  //  }
  //  def setdatas(indexs:Array[Int],vs:Array[Float]): Unit ={
  //    val len=indexs.length
  //    if(len==vs.length){
  //      for(i<- 0 to len-1){
  //        this.setdata(i,vs(i))
  //      }
  //    }
  //  }
  //  def setdatas(indexs:Array[Int],vs:Array[Byte]): Unit ={
  //    val len=indexs.length
  //    if(len==vs.length){
  //      for(i<- 0 to len-1){
  //        this.setdata(i,vs(i))
  //      }
  //    }
  //  }
  //  def setdatas(indexs:Array[Int],vs:Array[Int]): Unit ={
  //    val len=indexs.length
  //    if(len==vs.length){
  //      for(i<- 0 to len-1){
  //        this.setdata(i,vs(i))
  //      }
  //    }
  //  }
  //  def setdatas(indexs:Array[Int],vs:Array[Long]): Unit ={
  //    val len=indexs.length
  //    if(len==vs.length){
  //      for(i<- 0 to len-1){
  //        this.setdata(i,vs(i))
  //      }
  //    }
  //  }
  //  def setdatas(indexs:Array[Int],vs:Array[Short]): Unit ={
  //    val len=indexs.length
  //    if(len==vs.length){
  //      for(i<- 0 to len-1){
  //        this.setdata(i,vs(i))
  //      }
  //    }
  //  }

  def getdata(index:Int):Object ={
    val obj={
      DataType.getType(dataType) match {
        case DataType.INT => this.array.getInt(index)
        case DataType.SHORT =>  this.array.getShort(index)
        case DataType.FLOAT =>  this.array.getFloat(index)
        case DataType.DOUBLE =>  this.array.getDouble(index)
        case DataType.LONG => this.array.getLong(index)
        case DataType.BYTE =>  this.array.getByte(index)
        case badType => {
          throw new Exception("Can't convert ma2.Array[" + badType + "] to numeric array.")
          null
        }
      }
    }
    obj.asInstanceOf[Object]
  }

  def copy(): VariableCW = {
    val arraycopy=array.copy
    new VariableCW(name,  arraycopy, attributes.clone(), dims)
  }

  override def clone(): AnyRef = {
    val arraycopy=array.copy
    new VariableCW(name,arraycopy, attributes.clone(), dims)
  }



}
