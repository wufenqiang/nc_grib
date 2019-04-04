package com.weather.bigdata.it.nc_grib.ReadWrite

import java.io.InputStream
import java.util.Properties

import org.apache.log4j.Logger

object nc_grib_PropertiesUtil {
  val log:Logger=Logger.getRootLogger
  private val prop:java.util.Properties={
    val prop0 :Properties = new java.util.Properties()
    val loader = Thread.currentThread.getContextClassLoader()
    val loadfile: InputStream=loader.getResourceAsStream("nc_grib.properties")
    prop0.load(loadfile)
    this.log.info("load sparksubmit_config.properties")
    this.log.info(prop0)
    prop0
  }

  val tmp: String = this.prop.getProperty("tmp")

  val writeCDFVersion=this.prop.getProperty("writeCDFVersion")

  val timeName=this.prop.getProperty("timeName")


  val reftimeKey:String=timeName
}
