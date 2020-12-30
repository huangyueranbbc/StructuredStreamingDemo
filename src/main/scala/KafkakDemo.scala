import org.apache.spark.sql.SparkSession

/** *****************************************************************************
 * @date 2020-12-30 11:31 上午
 * @author: <a href=mailto:huangyr>黄跃然</a>
 * @Description: Structured Streaming + Kafka
 ******************************************************************************/
object KafkakDemo {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("StructuredNetworkWordCount")
      .master("local[1]")
      .getOrCreate()

    // Create DataFrame representing the stream of input lines from connection to localhost:9999
    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "10.2.111.54:9092")
      .option("subscribe", "kbssusertopic")
      .option("startingOffsets", """{"kbssusertopic":{"0":198423576}}""")
      .load()

    import spark.implicits._
    df.printSchema()
    val query = df.select($"key", $"value")
      .as[(String, String)].map(kv => kv._1 + " " + kv._2).as[String]
      .writeStream
      .outputMode("append")
      .option("checkpointLocation","./ckp")
      .format("csv")
      .option("path","test.csv")
      .start()

    query.awaitTermination()
  }

}
