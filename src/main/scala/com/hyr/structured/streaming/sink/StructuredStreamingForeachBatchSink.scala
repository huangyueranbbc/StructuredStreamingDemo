package com.hyr.structured.streaming.sink

import org.apache.spark.sql.SparkSession

/** *****************************************************************************
 *
 * @date 2020-12-31 10:02 上午
 * @author: <a href=mailto:huangyr>huangyr</a>
 * @Description: Structured Streaming + 将计算结果数据写入存储服务
 * *****************************************************************************/
object StructuredStreamingForeachBatchSink {

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

    // kafka消息的schema
    df.printSchema()
    val query = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "CAST(partition AS STRING)", "CAST(offset AS STRING)", "CAST(topic AS STRING)", "CAST(timestamp AS STRING)")
      .repartition(10)
      .writeStream
      .foreachBatch((dataSet, batchId) => {
        println("======> batchID:"+batchId)
        //dataSet.foreach( t=>{println(t)})
      })
      //.foreach(new MysqlWriter())
      .start()

    query.awaitTermination()


  }

}
