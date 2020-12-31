package com.hyr.structured.streaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.OutputMode

/** *****************************************************************************
 *
 * @date 2020-12-30 11:31 上午
 * @author: <a href=mailto:huangyr>黄跃然</a>
 * @Description: 单词次数统计
 * *****************************************************************************/
object WordCount {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("StructuredNetworkWordCount")
      .master("local[1]")
      .getOrCreate()

    // Create DataFrame representing the stream of input lines from connection to localhost:9999
    val lines = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .load()

    // Split the lines into words
    import spark.implicits._
    val words = lines.as[String].flatMap(_.split(" "))

    // Generate running word count
    val wordCounts = words.groupBy("value").count()

    // Start running the query that prints the running counts to the console
    val query = wordCounts.writeStream
      .outputMode(OutputMode.Complete())
      .format("console")
      .start()

    query.awaitTermination()
  }

}
