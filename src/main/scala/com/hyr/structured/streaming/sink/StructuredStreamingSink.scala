package com.hyr.structured.streaming.sink

import org.apache.spark.sql.SparkSession

/** *****************************************************************************
 * @date 2020-12-31 10:02 上午
 * @author: <a href=mailto:huangyr>黄跃然</a>
 * @Description: Structured Streaming + 将计算结果数据写入存储服务
 ******************************************************************************/
object StructuredStreamingSink {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName(this.getClass.getName)
      .master("local[1]")
      .getOrCreate()

    // Create DataFrame representing the stream of input lines from connection to localhost:9999
    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "10.2.111.54:9092")
      .option("subscribe", "kbssusertopic")
      .option("startingOffsets", "earliest")
      .load()

    df.printSchema()
    val query = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "CAST(partition AS STRING)", "CAST(offset AS STRING)", "CAST(topic AS STRING)", "CAST(timestamp AS STRING)")
      .writeStream
      .foreach(new MysqlWriter())
      .start()

    query.awaitTermination()


  }

}
