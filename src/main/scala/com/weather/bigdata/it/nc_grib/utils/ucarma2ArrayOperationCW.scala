package com.weather.bigdata.it.nc_grib.utils

import ucar.ma2.DataType

object ucarma2ArrayOperationCW {
  /*def setDouble(array:AnyRef,index:Int,value:Double): Unit ={
    if(array.isInstanceOf[ucar.ma2.Array]){
      val array0=array.asInstanceOf[ucar.ma2.Array]
      array0.setDouble(index,value)
    }else{
      if(array.isInstanceOf[Array[Double]]){
        val array0=array.asInstanceOf[Array[Double]]
        array0(index)=value
      }else{
        val e:Exception=new Exception("ucarma2ArrayOperationCW输入类型有误")
        e.printStackTrace()
      }
    }
  }*/
  def setDouble (array: Array[Double], index: Int, value: Double): Unit = {
    array(index) = value
  }

  def setFloat (array: Array[Float], index: Int, value: Float): Unit = {
    array(index) = value
  }

  def setInt (array: Array[Int], index: Int, value: Int): Unit = {
    array(index) = value
  }

  def setLong (array: Array[Long], index: Int, value: Long): Unit = {
    array(index) = value
  }

  def setShort (array: Array[Short], index: Int, value: Short): Unit = {
    array(index) = value
  }

  def setByte (array: Array[Byte], index: Int, value: Byte): Unit = {
    array(index) = value
  }

  def setObject (array: Object, index: Int, value: AnyVal): Unit = {
    if (value.isInstanceOf[Double]) {
      this.setObject(array, index, value.asInstanceOf[Double])
    } else if (value.isInstanceOf[Float]) {
      this.setObject(array, index, value.asInstanceOf[Float])
    } else if (value.isInstanceOf[Int]) {
      this.setObject(array, index, value.asInstanceOf[Int])
    } else if (value.isInstanceOf[Long]) {
      this.setObject(array, index, value.asInstanceOf[Long])
    } else if (value.isInstanceOf[Short]) {
      this.setObject(array, index, value.asInstanceOf[Short])
    } else if (value.isInstanceOf[Byte]) {
      this.setObject(array, index, value.asInstanceOf[Byte])
    } else {
      val e: Exception = new Exception("setObject输入value类型有误")
      e.printStackTrace( )
    }
  }

  def setObject (array: Object, index: Int, value: Double): Unit = {
    if (array.isInstanceOf[ucar.ma2.Array]) {
      array.asInstanceOf[ucar.ma2.Array].setObject(index, value)
    } else {
      if (array.isInstanceOf[Array[Double]]) {
        array.asInstanceOf[Array[Double]](index) = value
      } else {
        val e: Exception = new Exception("setObject输入类型有误")
        e.printStackTrace( )
      }
    }

  }

  def setObject (array: Object, index: Int, value: Float): Unit = {
    if (array.isInstanceOf[ucar.ma2.Array]) {
      array.asInstanceOf[ucar.ma2.Array].setObject(index, value)
    } else {
      if (array.isInstanceOf[Array[Float]]) {
        array.asInstanceOf[Array[Float]](index) = value
      } else {
        val e: Exception = new Exception("setObject输入类型有误")
        e.printStackTrace( )
      }
    }

  }

  def setObject (array: Object, index: Int, value: Int): Unit = {
    if (array.isInstanceOf[ucar.ma2.Array]) {
      array.asInstanceOf[ucar.ma2.Array].setObject(index, value)
    } else {
      if (array.isInstanceOf[Array[Int]]) {
        array.asInstanceOf[Array[Int]](index) = value
      } else {
        val e: Exception = new Exception("setObject输入类型有误")
        e.printStackTrace( )
      }
    }

  }

  def setObject (array: Object, index: Int, value: Long): Unit = {
    if (array.isInstanceOf[ucar.ma2.Array]) {
      array.asInstanceOf[ucar.ma2.Array].setObject(index, value)
    } else {
      if (array.isInstanceOf[Array[Long]]) {
        array.asInstanceOf[Array[Long]](index) = value
      } else {
        val e: Exception = new Exception("setObject输入类型有误")
        e.printStackTrace( )
      }
    }

  }

  def setObject (array: Object, index: Int, value: Short): Unit = {
    if (array.isInstanceOf[ucar.ma2.Array]) {
      array.asInstanceOf[ucar.ma2.Array].setObject(index, value)
    } else {
      if (array.isInstanceOf[Array[Short]]) {
        array.asInstanceOf[Array[Short]](index) = value
      } else {
        val e: Exception = new Exception("setObject输入类型有误")
        e.printStackTrace( )
      }
    }

  }

  def setObject (array: Object, index: Int, value: Byte): Unit = {
    if (array.isInstanceOf[ucar.ma2.Array]) {
      array.asInstanceOf[ucar.ma2.Array].setObject(index, value)
    } else {
      if (array.isInstanceOf[Array[Byte]]) {
        array.asInstanceOf[Array[Byte]](index) = value
      } else {
        val e: Exception = new Exception("setObject输入array类型有误")
        e.printStackTrace( )
      }
    }

  }

  def setObjects (array0: ucar.ma2.Array, dataType1: ucar.ma2.DataType): ucar.ma2.Array = {
    this.setObjects(array0, dataType1.toString)
  }

  def setObjects (array0: ucar.ma2.Array, dataType1Str: String): ucar.ma2.Array = {
    val dataType0: DataType = array0.getDataType
    val dataType0Str = dataType0.toString
    val len: Int = array0.getSize.toInt
    if (dataType0Str.equals(dataType1Str)) {
      array0
    } else {
      val dataType1 = ucar.ma2.DataType.getType(dataType1Str)

      val array1: ucar.ma2.Array = ucar.ma2.Array.factory(dataType1, array0.getShape)

      (dataType0, dataType1) match {
        case (DataType.INT, DataType.SHORT) => {
          for (i <- 0 to len - 1) {
            this.setShort(array1, i, array0.getInt(i).toShort)
          }
        }
        case (DataType.INT, DataType.FLOAT) => {
          for (i <- 0 to len - 1) {
            this.setFloat(array1, i, array0.getInt(i).toFloat)
          }
        }
        case (DataType.INT, DataType.DOUBLE) => {
          for (i <- 0 to len - 1) {
            this.setDouble(array1, i, array0.getInt(i).toDouble)
          }
        }
        case (DataType.INT, DataType.LONG) => {
          for (i <- 0 to len - 1) {
            this.setLong(array1, i, array0.getInt(i).toLong)
          }
        }
        case (DataType.INT, DataType.BYTE) => {
          for (i <- 0 to len - 1) {
            this.setByte(array1, i, array0.getInt(i).toByte)
          }
        }

        case (DataType.SHORT, DataType.INT) => {
          for (i <- 0 to len - 1) {
            this.setInt(array1, i, array0.getShort(i).toInt)
          }
        }
        case (DataType.SHORT, DataType.FLOAT) => {
          for (i <- 0 to len - 1) {
            this.setFloat(array1, i, array0.getShort(i).toFloat)
          }
        }
        case (DataType.SHORT, DataType.DOUBLE) => {
          for (i <- 0 to len - 1) {
            this.setDouble(array1, i, array0.getShort(i).toDouble)
          }
        }
        case (DataType.SHORT, DataType.LONG) => {
          for (i <- 0 to len - 1) {
            this.setLong(array1, i, array0.getShort(i).toLong)
          }
        }
        case (DataType.SHORT, DataType.BYTE) => {
          for (i <- 0 to len - 1) {
            this.setByte(array1, i, array0.getShort(i).toByte)
          }
        }

        case (DataType.FLOAT, DataType.INT) => {
          for (i <- 0 to len - 1) {
            this.setInt(array1, i, array0.getFloat(i).toInt)
          }
        }
        case (DataType.FLOAT, DataType.SHORT) => {
          for (i <- 0 to len - 1) {
            this.setShort(array1, i, array0.getFloat(i).toShort)
          }
        }
        case (DataType.FLOAT, DataType.DOUBLE) => {
          for (i <- 0 to len - 1) {
            this.setDouble(array1, i, array0.getFloat(i).toDouble)
          }
        }
        case (DataType.FLOAT, DataType.LONG) => {
          for (i <- 0 to len - 1) {
            this.setLong(array1, i, array0.getFloat(i).toLong)
          }
        }
        case (DataType.FLOAT, DataType.BYTE) => {
          for (i <- 0 to len - 1) {
            this.setByte(array1, i, array0.getFloat(i).toByte)
          }
        }

        case (DataType.DOUBLE, DataType.INT) => {
          for (i <- 0 to len - 1) {
            this.setInt(array1, i, array0.getDouble(i).toInt)
          }
        }
        case (DataType.DOUBLE, DataType.SHORT) => {
          for (i <- 0 to len - 1) {
            this.setShort(array1, i, array0.getDouble(i).toShort)
          }
        }
        case (DataType.DOUBLE, DataType.FLOAT) => {
          for (i <- 0 to len - 1) {
            this.setFloat(array1, i, array0.getDouble(i).toFloat)
          }
        }
        case (DataType.DOUBLE, DataType.LONG) => {
          for (i <- 0 to len - 1) {
            this.setLong(array1, i, array0.getDouble(i).toLong)
          }
        }
        case (DataType.DOUBLE, DataType.BYTE) => {
          for (i <- 0 to len - 1) {
            this.setByte(array1, i, array0.getDouble(i).toByte)
          }
        }

        case (DataType.LONG, DataType.INT) => {
          for (i <- 0 to len - 1) {
            this.setInt(array1, i, array0.getLong(i).toInt)
          }
        }
        case (DataType.LONG, DataType.SHORT) => {
          for (i <- 0 to len - 1) {
            this.setShort(array1, i, array0.getLong(i).toShort)
          }
        }
        case (DataType.LONG, DataType.FLOAT) => {
          for (i <- 0 to len - 1) {
            this.setFloat(array1, i, array0.getLong(i).toFloat)
          }
        }
        case (DataType.LONG, DataType.DOUBLE) => {
          for (i <- 0 to len - 1) {
            this.setDouble(array1, i, array0.getLong(i).toDouble)
          }
        }
        case (DataType.LONG, DataType.BYTE) => {
          for (i <- 0 to len - 1) {
            this.setByte(array1, i, array0.getLong(i).toByte)
          }
        }

        case (DataType.BYTE, DataType.INT) => {
          for (i <- 0 to len - 1) {
            this.setInt(array1, i, array0.getByte(i).toInt)
          }
        }
        case (DataType.BYTE, DataType.SHORT) => {
          for (i <- 0 to len - 1) {
            this.setShort(array1, i, array0.getByte(i).toShort)
          }
        }
        case (DataType.BYTE, DataType.FLOAT) => {
          for (i <- 0 to len - 1) {
            this.setFloat(array1, i, array0.getByte(i).toFloat)
          }
        }
        case (DataType.BYTE, DataType.DOUBLE) => {
          for (i <- 0 to len - 1) {
            this.setDouble(array1, i, array0.getByte(i).toDouble)
          }
        }
        case (DataType.BYTE, DataType.LONG) => {
          for (i <- 0 to len - 1) {
            this.setLong(array1, i, array0.getByte(i).toLong)
          }
        }

        case badType => throw new Exception("Can't convert,dataType0Str=" + dataType0Str + ";dataType1Str=" + dataType1Str)
      }
      /*dataType0 match {
        case DataType.INT => {
          dataType1 match {
            case DataType.SHORT=>{
              for(i<- 0 to len-1){
                this.setShort(array1,i,array0.getInt(i).toShort)
              }
            }
            case DataType.FLOAT=>{
              for(i<- 0 to len-1){
                this.setFloat(array1,i,array0.getInt(i).toFloat)
              }
            }
            case DataType.DOUBLE=>{
              for(i<- 0 to len-1){
                this.setDouble(array1,i,array0.getInt(i).toDouble)
              }
            }
            case DataType.LONG=>{
              for(i<- 0 to len-1){
                this.setLong(array1,i,array0.getInt(i).toLong)
              }
            }
            case DataType.BYTE =>{
              for(i<- 0 to len-1){
                this.setByte(array1,i,array0.getInt(i).toByte)
              }
            }
            case badType =>throw new Exception("Can't convert,dataType0Str="+dataType0Str+";dataType1Str="+dataType1Str)
          }
        }
        case DataType.SHORT =>{
          dataType1 match {
            case DataType.INT=>{
              for(i<- 0 to len-1){
                this.setInt(array1,i,array0.getShort(i).toInt)
              }
            }
            case DataType.FLOAT=>{
              for(i<- 0 to len-1){
                this.setFloat(array1,i,array0.getShort(i).toFloat)
              }
            }
            case DataType.DOUBLE=>{
              for(i<- 0 to len-1){
                this.setDouble(array1,i,array0.getShort(i).toDouble)
              }
            }
            case DataType.LONG=>{
              for(i<- 0 to len-1){
                this.setLong(array1,i,array0.getShort(i).toLong)
              }
            }
            case DataType.BYTE =>{
              for(i<- 0 to len-1){
                this.setByte(array1,i,array0.getShort(i).toByte)
              }
            }
            case badType =>throw new Exception("Can't convert,dataType0Str="+dataType0Str+";dataType1Str="+dataType1Str)
          }
        }
        case DataType.FLOAT => {
          dataType1 match {
            case DataType.INT=>{
              for(i<- 0 to len-1){
                this.setShort(array1,i,array0.getInt(i).toShort)
              }
            }
            case DataType.SHORT=>{
              for(i<- 0 to len-1){
                this.setFloat(array1,i,array0.getInt(i).toFloat)
              }
            }
            case DataType.DOUBLE=>{
              for(i<- 0 to len-1){
                this.setDouble(array1,i,array0.getInt(i).toDouble)
              }
            }
            case DataType.LONG=>{
              for(i<- 0 to len-1){
                this.setLong(array1,i,array0.getInt(i).toLong)
              }
            }
            case DataType.BYTE =>{
              for(i<- 0 to len-1){
                this.setByte(array1,i,array0.getInt(i).toByte)
              }
            }
            case badType =>throw new Exception("Can't convert,dataType0Str="+dataType0Str+";dataType1Str="+dataType1Str)
          }
        }
        case DataType.DOUBLE => {
          dataType1 match {
            case DataType.INT=>{
              for(i<- 0 to len-1){
                this.setShort(array1,i,array0.getInt(i).toShort)
              }
            }
            case DataType.SHORT=>{
              for(i<- 0 to len-1){
                this.setFloat(array1,i,array0.getInt(i).toFloat)
              }
            }
            case DataType.FLOAT=>{
              for(i<- 0 to len-1){
                this.setDouble(array1,i,array0.getInt(i).toDouble)
              }
            }
            case DataType.LONG=>{
              for(i<- 0 to len-1){
                this.setLong(array1,i,array0.getInt(i).toLong)
              }
            }
            case DataType.BYTE =>{
              for(i<- 0 to len-1){
                this.setByte(array1,i,array0.getInt(i).toByte)
              }
            }
            case badType =>throw new Exception("Can't convert,dataType0Str="+dataType0Str+";dataType1Str="+dataType1Str)
          }
        }
        case DataType.LONG => {
          dataType1 match {
            case DataType.INT=>{
              for(i<- 0 to len-1){
                this.setShort(array1,i,array0.getInt(i).toShort)
              }
            }
            case DataType.SHORT=>{
              for(i<- 0 to len-1){
                this.setFloat(array1,i,array0.getInt(i).toFloat)
              }
            }
            case DataType.FLOAT=>{
              for(i<- 0 to len-1){
                this.setDouble(array1,i,array0.getInt(i).toDouble)
              }
            }
            case DataType.DOUBLE=>{
              for(i<- 0 to len-1){
                this.setLong(array1,i,array0.getInt(i).toLong)
              }
            }
            case DataType.BYTE =>{
              for(i<- 0 to len-1){
                this.setByte(array1,i,array0.getInt(i).toByte)
              }
            }
            case badType =>throw new Exception("Can't convert,dataType0Str="+dataType0Str+";dataType1Str="+dataType1Str)
          }
        }
        case DataType.BYTE => {
          dataType1 match {
            case DataType.INT=>{
              for(i<- 0 to len-1){
                this.setShort(array1,i,array0.getInt(i).toShort)
              }
            }
            case DataType.SHORT=>{
              for(i<- 0 to len-1){
                this.setFloat(array1,i,array0.getInt(i).toFloat)
              }
            }
            case DataType.FLOAT=>{
              for(i<- 0 to len-1){
                this.setDouble(array1,i,array0.getInt(i).toDouble)
              }
            }
            case DataType.DOUBLE=>{
              for(i<- 0 to len-1){
                this.setLong(array1,i,array0.getInt(i).toLong)
              }
            }
            case DataType.LONG =>{
              for(i<- 0 to len-1){
                this.setByte(array1,i,array0.getInt(i).toByte)
              }
            }
            case badType =>throw new Exception("Can't convert,dataType0Str="+dataType0Str+";dataType1Str="+dataType1Str)
          }
        }
        case badType => throw new Exception("Can't convert,dataType0Str="+dataType0Str+";dataType1Str="+dataType1Str)
      }*/


      array1
    }
  }

  def setDouble (array: ucar.ma2.Array, index: Int, value: Double): Unit = {
    array.setDouble(index, value)
  }

  def setFloat (array: ucar.ma2.Array, index: Int, value: Float): Unit = {
    array.setFloat(index, value)
    //    ucar.ma2.Index
  }

  def setInt (array: ucar.ma2.Array, index: Int, value: Int): Unit = {
    array.setInt(index, value)
  }

  def setLong (array: ucar.ma2.Array, index: Int, value: Long): Unit = {
    array.setLong(index, value)
  }

  def setShort (array: ucar.ma2.Array, index: Int, value: Short): Unit = {
    array.setShort(index, value)
  }

  def setByte (array: ucar.ma2.Array, index: Int, value: Byte): Unit = {
    array.setByte(index, value)
  }
}
