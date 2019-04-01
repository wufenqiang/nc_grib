package com.weather.bigdata.it.nc_grib.utils

object IndexOperation {
  //  def getIndex4_thlatlon(timeIndex: Int, heightIndex:Int, latIndex: Int, lonIndex: Int, heightLen:Int, latLen: Int, lonLen: Int): Int = timeIndex * heightLen * latLen * lonLen + heightIndex * latLen * lonLen + latIndex * lonLen + lonIndex
  //  def getIndexs4_thlatlon(timeIndexs:Array[Int], heightIndex:Int, latIndex: Int, lonIndex: Int, heightLen:Int, latLen: Int, lonLen: Int): Array[Int] =timeIndexs.map(timeIndex=>this.getIndex4_thlatlon(timeIndex,heightIndex,latIndex, lonIndex, heightLen, latLen, lonLen))
  //
  //  def getIndex3_hlatlon(heightIndex: Int, latIndex: Int, lonIndex: Int, latLen: Int, lonLen: Int): Int = heightIndex * latLen * lonLen +  latIndex * lonLen + lonIndex
  //  def getIndexs3_hlatlon(heightIndexs: Array[Int], latIndex: Int, lonIndex: Int, latLen: Int, lonLen: Int):Array[Int]=heightIndexs.map(heightIndex=>this.getIndex3_hlatlon(heightIndex,latIndex, lonIndex,  latLen, lonLen))
  //  def getIndex3_tlatlon(timeIndex: Int, latIndex: Int, lonIndex: Int, latLen: Int, lonLen: Int): Int = timeIndex * latLen * lonLen +  latIndex * lonLen + lonIndex
  //  def getIndexs3_tlatlon(timeIndexs: Array[Int], latIndex: Int, lonIndex: Int, latLen: Int, lonLen: Int): Array[Int] =timeIndexs.map(timeIndex=>this.getIndex3_tlatlon(timeIndex,latIndex, lonIndex,  latLen, lonLen))
  //
  //

  def getIndexs4_thlatlon (timeIndexs: Array[Int], heightIndex: Int, latIndex: Int, lonIndex: Int, heightLen: Int, latLen: Int, lonLen: Int): Array[Int] = timeIndexs.map(timeIndex => this.getIndex4_thlatlon(timeIndex, heightIndex, latIndex, lonIndex, heightLen, latLen, lonLen))

  def getIndexs4_thlatlon (timeIndex: Int, heightIndexs: Array[Int], latIndex: Int, lonIndex: Int, heightLen: Int, latLen: Int, lonLen: Int): Array[Int] = heightIndexs.map(heightIndex => this.getIndex4_thlatlon(timeIndex, heightIndex, latIndex, lonIndex, heightLen, latLen, lonLen))

  def getIndexs4_thlatlon (timeIndex: Int, heightIndex: Int, latIndexs: Array[Int], lonIndexs: Array[Int], heightLen: Int, latLen: Int, lonLen: Int): Array[Array[Int]] = {
    latIndexs.map(latIndex => {
      lonIndexs.map(lonIndex => {
        this.getIndex4_thlatlon(timeIndex, heightIndex, latIndex, lonIndex, heightLen, latLen, lonLen)
      })
    })
  }

  def getIndex4_thlatlon (timeIndex: Int, heightIndex: Int, latIndex: Int, lonIndex: Int, heightLen: Int, latLen: Int, lonLen: Int): Int = timeIndex * heightLen * latLen * lonLen + heightIndex * latLen * lonLen + latIndex * lonLen + lonIndex

  def getIndexs3_hlatlon (heightIndexs: Array[Int], latIndex: Int, lonIndex: Int, latLen: Int, lonLen: Int): Array[Int] = heightIndexs.map(heightIndex => this.getIndex3_hlatlon(heightIndex, latIndex, lonIndex, latLen, lonLen))

  def getIndex3_hlatlon (heightIndex: Int, latIndex: Int, lonIndex: Int, latLen: Int, lonLen: Int): Int = heightIndex * latLen * lonLen + latIndex * lonLen + lonIndex

  def getIndexs3_tlatlon (timeIndexs: Array[Int], latIndex: Int, lonIndex: Int, latLen: Int, lonLen: Int): Array[Int] = timeIndexs.map(timeIndex => this.getIndex3_tlatlon(timeIndex, latIndex, lonIndex, latLen, lonLen))

  def getIndex3_tlatlon (timeIndex: Int, latIndex: Int, lonIndex: Int, latLen: Int, lonLen: Int): Int = timeIndex * latLen * lonLen + latIndex * lonLen + lonIndex

  def getIndexs2_latlon (latIndexs: Array[Int], lonIndex: Int, lonLen: Int): Array[Int] = latIndexs.map(latIndex => this.getIndex2_latlon(latIndex, lonIndex, lonLen))

  def getIndex2_latlon (latIndex: Int, lonIndex: Int, lonLen: Int): Int = latIndex * lonLen + lonIndex
}
