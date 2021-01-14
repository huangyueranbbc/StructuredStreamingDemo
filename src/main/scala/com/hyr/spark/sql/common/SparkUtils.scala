package com.hyr.spark.sql.common

import org.apache.spark.sql.SparkSession

/** *****************************************************************************
 * @date 2021-01-14 2:31 下午
 * @author: <a href=mailto:huangy>huangyr</a>
 * @Description: spark工具类
 ******************************************************************************/
object SparkUtils {

  def createSparkSession(className: String): SparkSession = {
    val spark = SparkSession.builder()
      .appName(className)
      .config("spark.some.config.option", "some-value")
      .master("local[1]")
      .getOrCreate()
    spark
  }

}
