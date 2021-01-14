package com.hyr.structured.streaming.source

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.StreamingQueryListener.{QueryProgressEvent, QueryStartedEvent, QueryTerminatedEvent}
import org.apache.spark.sql.streaming.{OutputMode, StreamingQueryListener, Trigger}

/** *****************************************************************************
 *
 * @date 2020-12-30 11:31 上午
 * @author: <a href=mailto:huangyr>huangyr</a>
 * @Description: Structured Streaming + Kafka 消费kafka数据源写入csv
 * *****************************************************************************/
object KafkaSource {

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

    // 添加监听器，每一批次处理完成，将该批次的相关信息，如起始offset，抓取记录数量，处理时间打印到控制台
    spark.streams.addListener(new StreamingQueryListener() {
      override def onQueryStarted(queryStarted: QueryStartedEvent): Unit = {
        println("Query started: " + queryStarted.id)
      }

      override def onQueryTerminated(queryTerminated: QueryTerminatedEvent): Unit = {
        println("Query terminated: " + queryTerminated.id)
      }

      override def onQueryProgress(queryProgress: QueryProgressEvent): Unit = {
        println("Query made progress: " + queryProgress.progress)
      }
    })

    import spark.implicits._
    df.printSchema()

    val query = df.select($"key", $"value")
      .as[(String, String)].map(kv => kv._1 + " " + kv._2).as[String]
      .writeStream
      .outputMode(OutputMode.Append()) // 每次写入新行
      .trigger(Trigger.ProcessingTime(0)) // 触发时间间隔,0表示尽可能的快
      .option("checkpointLocation", "./checkpoint1") // 设置checkpoint目录
      .format("csv") // 写入csv
      .option("path", "csv")
      .start()

    query.awaitTermination()
    spark.stop()
  }

}
