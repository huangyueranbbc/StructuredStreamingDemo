package com.hyr.spark.sql

import com.hyr.spark.sql.common.SparkUtils
import org.apache.spark.sql.SparkSession

/** *****************************************************************************
 * @date 2021-01-11 2:03 下午
 * @author: <a href=mailto:huangy>huangyr</a>
 * @Description: spark-sql基本操作 http://spark.apache.org/docs/2.4.0/sql-programming-guide.html
 * *****************************************************************************/
object BasicDataFrameExample {

  def main(args: Array[String]): Unit = {
    // create SparkSession
    val spark = SparkUtils.createSparkSession(this.getClass.getName)

    // create DataFrame
    val df = spark.read.json("src/main/resources/people.json")
    import spark.implicits._
    df.show(false)

    // Print the schema in a tree format
    df.printSchema()
    // root
    // |-- age: long (nullable = true)
    // |-- name: string (nullable = true)

    // Select only the "name" column
    df.select("name").show()
    // +-------+
    // |   name|
    // +-------+
    // |Michael|
    // |   Andy|
    // | Justin|
    // +-------+

    // Select everybody, but increment the age by 1
    df.select($"name", $"age" + 1).show()
    // +-------+---------+
    // |   name|(age + 1)|
    // +-------+---------+
    // |Michael|     null|
    // |   Andy|       31|
    // | Justin|       20|
    // +-------+---------+

    // Select people older than 21
    df.filter($"age" > 21).show()
    // +---+----+
    // |age|name|
    // +---+----+
    // | 30|Andy|
    // +---+----+

    // Count people by age
    df.groupBy("age").count().show()
    // +----+-----+
    // | age|count|
    // +----+-----+
    // |  19|    1|
    // |null|    1|
    // |  30|    1|
    // +----+-----+
    // $example off:untyped_ops$

    // $example on:run_sql$
    // Register the DataFrame as a SQL temporary view
    spark.sql("show tables").show(false)
    df.createOrReplaceTempView("people")
    spark.sql("show tables").show(false)

    val sqlDF = spark.sql("SELECT * FROM people")
    sqlDF.show()
    // +----+-------+
    // | age|   name|
    // +----+-------+
    // |null|Michael|
    // |  30|   Andy|
    // |  19| Justin|
    // +----+-------+
    // $example off:run_sql$

    // $example on:global_temp_view$
    // 全局表,在程序运行期间,允许多个session会话访问。Register the DataFrame as a global temporary view
    df.createGlobalTempView("people")
    spark.sql("show tables").show(false)
    // Global temporary view is tied to a system preserved database `global_temp`
    spark.sql("SELECT * FROM global_temp.people").show()
    // Global temporary view is cross-session
    spark.newSession().sql("SELECT * FROM global_temp.people").show()

    spark.stop()
  }

}
