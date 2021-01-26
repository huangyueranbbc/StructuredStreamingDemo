package com.hyr.spark.sql.hive

import java.io.File

import com.hyr.spark.sql.utils.Record
import org.apache.spark.sql.{Row, SaveMode, SparkSession}

/** *****************************************************************************
 *
 * @date 2021-01-22 1:22 下午
 * @author: <a href=mailto:huangyr>huangyr</a>
 * @Description: spark-hive
 *               Spark SQL also supports reading and writing data stored in Apache Hive. However, since Hive has a large number of dependencies,
 *               these dependencies are not included in the default Spark distribution. If Hive dependencies can be found on the classpath,
 *               Spark will load them automatically. Note that these Hive dependencies must also be present on all of the worker nodes,
 *               as they will need access to the Hive serialization and deserialization libraries (SerDes) in order to access data stored in Hive.
 *
 *               Configuration of Hive is done by placing your hive-site.xml, core-site.xml (for security configuration),
 *               and hdfs-site.xml (for HDFS configuration) file in conf/.
 *
 *               When working with Hive, one must instantiate SparkSession with Hive support, including connectivity to a persistent Hive metastore,
 *               support for Hive serdes, and Hive user-defined functions. Users who do not have an existing Hive deployment can still enable Hive support.
 *               When not configured by the hive-site.xml, the context automatically creates metastore_db in the current directory and creates a directory
 *               configured by spark.sql.warehouse.dir, which defaults to the directory spark-warehouse in the current directory that the Spark application is started.
 *               Note that the hive.metastore.warehouse.dir property in hive-site.xml is deprecated since Spark 2.0.0. Instead, use spark.sql.warehouse.dir to specify
 *               he default location of database in warehouse. You may need to grant write privilege to the user who starts the Spark application.
 * *****************************************************************************/
object SparkHiveExample {

  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hive");
    // 本地运行需添加配置hive-site.xml。
    val warehouseLocation = new File("spark-warehouse").getAbsolutePath

    val spark = SparkSession
      .builder()
      .appName("Spark Hive Example")
      // 如不设置spark.sql.warehouse.dir,会在本地采集metastore_db目录
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport()
      .master("local[1]")
      .getOrCreate()

    import spark.implicits._
    import spark.sql

    sql("show databases").show(false)

    // 导入数据
    sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING) USING hive")
    sql("LOAD DATA LOCAL INPATH 'src/main/resources/kv1.txt' INTO TABLE src")

    sql("SELECT * FROM src").show(numRows = 10, truncate = false)
    sql("SELECT COUNT(*) FROM src").show()

    // create dataframe
    val sqlDataFrame = sql("SELECT key, value FROM src WHERE key < 10 ORDER BY key")

    val stringsDataSet = sqlDataFrame.map {
      case Row(key: Int, value: String) => s"Key: $key, Value: $value"
    }.filter(t => t != null)
    stringsDataSet.show(numRows = 10, truncate = false)

    // 使用DataFrames在SparkSession中创建临时视图
    val recordsDF = spark.createDataFrame((1 to 100).map(i => Record(i, s"val_$i")))
    recordsDF.createOrReplaceTempView("records")
    sql("SELECT * FROM records r JOIN src s ON r.key = s.key").show()


    sql("CREATE TABLE hive_records(key int, value string) STORED AS PARQUET")
    // Save DataFrame to the Hive managed table
    val df = spark.table("src")
    df.write.mode(SaveMode.Overwrite).saveAsTable("hive_records")
    // After insertion, the Hive managed table has data now
    sql("SELECT * FROM hive_records").show()

    val dataDir = "/tmp/parquet_data"
    spark.range(10).write.parquet(dataDir)
    // Create a Hive external Parquet table
    sql(s"CREATE EXTERNAL TABLE hive_ints(key int) STORED AS PARQUET LOCATION '$dataDir'")
    // The Hive external table should already have data
    sql("SELECT * FROM hive_ints").show()

    // 动态分区
    spark.sqlContext.setConf("hive.exec.dynamic.partition", "true")
    spark.sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
    // Create a Hive partitioned table using DataFrame API
    df.write.partitionBy("key").format("hive").saveAsTable("hive_part_tbl")
    // Partitioned column `key` will be moved to the end of the schema.
    sql("SELECT * FROM hive_part_tbl").show()

    // broadcast 广播
    import org.apache.spark.sql.functions.broadcast
    broadcast(spark.table("src")).join(spark.table("hive_records"), "key").show()

    spark.stop()
  }

}
