import java.io.File

import org.apache.spark.sql.SparkSession

/** *****************************************************************************
 *
 * @date 2021-01-26 2:22 下午
 * @author: <a href=mailto:huangyr>huangyr</a>
 * @Description:
 * *****************************************************************************/
object HiveTest {

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

    import org.apache.spark.sql.functions.broadcast
    broadcast(spark.table("src")).join(spark.table("hive_records"), "key").show()

    sql("use fds_dim")
    sql("show tables").show()
  }

}
