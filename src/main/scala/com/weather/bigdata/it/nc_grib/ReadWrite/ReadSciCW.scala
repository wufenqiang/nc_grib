package com.weather.bigdata.it.nc_grib.ReadWrite

import java.util.Date

import com.weather.bigdata.it.nc_grib.core.SciDatasetCW
import com.weather.bigdata.it.utils.formatUtil.DateFormatUtil
import com.weather.bigdata.it.utils.hdfsUtil.{HDFSFile, HDFSOperation1}
import org.apache.hadoop.fs.Path
import org.dia.utils.NetCDFUtils
import ucar.nc2.dataset.NetcdfDataset


object ReadSciCW {
  //  val hdfs: FileSystem =FileSystem.get(conf)

  def read (fileName: String, vars: List[String]): SciDatasetCW = {
    val N: Int = 3
    val scidata = this.read_recycle(fileName, vars, 3)
    if (scidata == null) {
      val e: Exception = new Exception("单点读取nc失败,N=" + N + ".建议调大N,fileName=" + fileName)
      e.printStackTrace( )
    }
    scidata
  }

  def read (fileName: String, vars: Array[String]): SciDatasetCW = {
    this.read(fileName, vars.toList)
  }

  def main (args: Array[String]): Unit = {


    //    val inputPath = {
    //      if (PropertiesUtil.isPrd) {
    //        args(0)
    //      } else {
    //        "/D:/Data/forecast/Output/NMC/20180308/08/WIND/1h/"
    //      }
    //    }
    //    val outputPath = {
    //      if (PropertiesUtil.isPrd) {
    //        args(1)
    //      } else {
    //        "/D:/Data/forecast/tmp2/"
    //      }
    //    }
    //    val select = args(2)
    //    if (select.equals("SciDataset")) {
    //      println("SciDataset")
    //      val ssc = ContextUtil.getSciSparkContextCW( )
    //      val rdd: RDD[SciDatasetCW] = ssc.sciDatasets(inputPath, Nil, 77)
    //      WriteNcCW.WriteNcFile_test(rdd, outputPath)
    //    } else if (select.equals("SciDataset2")) {
    //      println("SciDataset2")
    //      val ssc2 = ContextUtil.getSciSparkContextCW( )
    //      val rdd2: RDD[SciDatasetCW] = ssc2.sciDatasets(inputPath, Nil, 77)
    //      WriteNcCW.WriteNcFile_test(rdd2, outputPath)
    //    }


    /*val ncArr={
      val path={
        if(PropertiesUtil.isPrd){
          "hdfs://dataflow-node-1:9000/data/dataSource/test/"
        }else{
          "/D:/Data/forecast/Output/NMC/20180308/08/WIND/1h/"
        }
      }
      val files={
        if(PropertiesUtil.isPrd){
          Array(
            "1.0_1.0_24.0_CLOUD_77.nc",
            "1.0_1.0_24.0_PRE_77.nc",
            "1.0_1.0_24.0_PRS_77.nc",
            "1.0_1.0_24.0_RHU_77.nc",
            "1.0_1.0_24.0_TEM_77.nc",
            "1.0_1.0_24.0_WIND_77.nc",
            "25.0_1.0_48.0_CLOUD_77.nc",
            "25.0_1.0_48.0_PRE_77.nc",
            "25.0_1.0_48.0_RHU_77.nc",
            "25.0_1.0_48.0_TEM_77.nc",
            "49.0_1.0_72.0_CLOUD_77.nc",
            "49.0_1.0_72.0_PRE_77.nc",
            "49.0_1.0_72.0_RHU_77.nc",
            "49.0_1.0_72.0_TEM_77.nc"
          )
        }else{
          Array("A49_WIND_1.0.nc","B49_WIND_1.0.nc","B50_WIND_1.0.nc")
        }
      }
      files.map(s=>path+s)
    }
    val ncRDD:RDD[String]=sc.makeRDD(ncArr,ncArr.length)
    val ouputPath:String={
      if(PropertiesUtil.isPrd){
        "hdfs://dataflow-node-1:9000/tmp/"
      }else{
        "/D:/Data/forecast/tmp2/"
      }
    }

    val (timeName,heightName,latName,lonName)=PropertiesUtil.getVarName()
    ncRDD.foreach(ncFileName=>{

      val scidata2:SciDataset2=this.read_private(ncFileName)
      println(new Date+"，读取结束:"+ncFileName)
      scidata2.variables.foreach(f=>{

        val varName=f._1
        val variable2=f._2
        val arr=variable2.array
        val dataType=variable2.dataType
        val dims=variable2.dims
        dims.foreach(s=>{
          println(s._1+";"+s._2)
        })
        /* val heightLen:Int=dims.get(heightName).get
         val latLen:Int=dims.get(latName).get
         val lonLen:Int=dims.get(lonName).get
         val index:Int=VariableOperation.getIndex4_thlatlon(i,j,k,l,heightLen,latLen,lonLen)*/
        val index=0

        if(dataType=="double"){
          arr.setDouble(index,Constant.doubleDefault)
        }else if(dataType=="float"){
          arr.setFloat(index,Constant.floatDefault)
        }else if(dataType=="byte"){
          arr.setByte(index,Constant.byteDefault)
        }

      })
      WriteFile2.WriteNcFile_test(scidata2,ouputPath)
      //      scidata2.writeToNetCDF(scidata2.datasetName,ouputPath)
    })*/

  }

  /*private def readBuffer(ncfile:String): Int ={
    var fileSize:Long={
      if(PropertiesUtil.isPrd){
        HDFSOperation.filesizeMB(ncfile)
      }else{
        FileOperation.filesizeMB(ncfile)
      }
    }
    if(fileSize<200){
      4000
    }else{
      8000
    }
  }*/
  private def read_private (ncFileName: String, vars: List[String]): SciDatasetCW = {

    //对本地输入文件名前加/做判断及\\转译/
    val fileName: String = {
      val fileName1 = ncFileName.replace("\\", "/")
      //      if (fileName1.startsWith("/")) {
      //        fileName1
      //      } else {
      //        "/" + fileName1
      //      }
      fileName1
    }
    val path = new Path(fileName)

    if (!HDFSOperation1.exists(fileName)) {
      val e: Exception = new Exception(fileName + "不存在,返回SciDataset为空指针")
      e.printStackTrace( )
      null
    } else {

      val fileSize: Long = {
        HDFSFile.filesizeKB(fileName)
      }
      val bufferSize: Int = {
        if (fileSize < 200 * 1024) {
          4000
        } else {
          8000
        }
      }

      println(new Date + ";bufferSize=" + bufferSize)


      val ncName = path.getName
      val pathName = fileName.replace(ncName, "")
      var k: NetcdfDataset = null


      try {
        k = NetCDFUtils.loadDFSNetCDFDataSet(pathName, fileName, bufferSize)
        println(DateFormatUtil.YYYYMMDDHHMMSSStr1(new Date) + ";ncFileName=" + ncFileName + ";k计算完毕,fileSize=" + fileSize + "KB")
        //        k.getVariables
        val result: SciDatasetCW = vars match {
          case Nil => new SciDatasetCW(k)
          case s: List[String] => new SciDatasetCW(k, s)
        }
        k.close( )
        k = null
        result
      } catch {
        case e: Exception => {
          val msg = "文件破损.fileName=" + fileName
          val e0 = new Exception(msg + ";" + e)
          e0.printStackTrace
          null
        }
      } finally {
        if (k != null) {
          k.close
        }
      }
    }
  }

  private def read_recycle (fileName: String, vars: List[String], n: Int): SciDatasetCW = {
    val scidata: SciDatasetCW = this.read_private(fileName, vars)
    if (scidata == null) {
      if (n == 0) {
        scidata
      } else {
        Thread.sleep(500)
        this.read_recycle(fileName, vars, n - 1)
      }
    } else {
      scidata
    }
  }
}

