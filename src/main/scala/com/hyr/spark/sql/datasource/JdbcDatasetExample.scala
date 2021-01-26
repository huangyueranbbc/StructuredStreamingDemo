package com.hyr.spark.sql.datasource

import com.hyr.spark.sql.common.SparkUtils
import org.apache.spark.sql.jdbc.{JdbcDialect, JdbcDialects}

/** *****************************************************************************
 *
 * @date 2021-01-18 2:06 下午
 * @author: <a href=mailto:huangyr>huangyr</a>
 * @Description: 通过JDBC的方式连接查询
 * 1.添加依赖
 *               <!-- https://mvnrepository.com/artifact/org.apache.hive/hive-jdbc -->
 *               <dependency>
 *                  <groupId>org.apache.hive</groupId>
 *                  <artifactId>hive-jdbc</artifactId>
 *                  <version>1.1.0-cdh5.16.99</version>
 *               </dependency>
 * 2.该依赖和spark-hive冲突
 ******************************************************************************/
@Deprecated
object JdbcDatasetExample {

  def main(args: Array[String]): Unit = {

    val spark = SparkUtils.createSparkSession(this.getClass.getName)

    // $example on:jdbc_dataset$
    // Note: JDBC loading and saving can be achieved via either the load/save or jdbc methods
    // Loading data from a JDBC source
    new RegisterHiveSqlDialect().register()
    val jdbcDF = spark.read
      .format("jdbc")
      .option("url", "jdbc:hive2://10.5.96.106:10000")
      .option("dbtable", "(select * from billdb.tra_pro_label) as a")
      .option("driver", "org.apache.hive.jdbc.HiveDriver")
      .option("user", "asset_app")
      // 必须添加，否则无数据返回
      .option("fetchsize", "100")
      .load()

    jdbcDF.show(numRows = 100, truncate = true)
    //
    //    val connectionProperties = new Properties()
    //    connectionProperties.put("user", "username")
    //    connectionProperties.put("password", "password")
    //    val jdbcDF2 = spark.read
    //      .jdbc("jdbc:postgresql:dbserver", "schema.tablename", connectionProperties)
    //    // Specifying the custom data types of the read schema
    //    connectionProperties.put("customSchema", "id DECIMAL(38, 0), name STRING")
    //    val jdbcDF3 = spark.read
    //      .jdbc("jdbc:postgresql:dbserver", "schema.tablename", connectionProperties)
    //
    //    // Saving data to a JDBC source
    //    jdbcDF.write
    //      .format("jdbc")
    //      .option("url", "jdbc:postgresql:dbserver")
    //      .option("dbtable", "schema.tablename")
    //      .option("user", "username")
    //      .option("password", "password")
    //      .save()
    //
    //    jdbcDF2.write
    //      .jdbc("jdbc:postgresql:dbserver", "schema.tablename", connectionProperties)
    //
    //    // Specifying create table column data types on write
    //    jdbcDF.write
    //      .option("createTableColumnTypes", "name CHAR(64), comments VARCHAR(1024)")
    //      .jdbc("jdbc:postgresql:dbserver", "schema.tablename", connectionProperties)
    //    // $example off:jdbc_dataset$
  }

  /**
   * hive-jdbc版本过低,添加spark-hive—jdbc支持
   */
  class RegisterHiveSqlDialect {
    def register(): Unit = {
      JdbcDialects.registerDialect(HiveSqlDialect)
    }
  }

  case object HiveSqlDialect extends JdbcDialect {
    override def canHandle(url: String): Boolean = url.startsWith("jdbc:hive2")

    override def quoteIdentifier(colName: String): String = {
      colName.split('.').map(part => s"`$part`").mkString(".")
    }
  }

}
