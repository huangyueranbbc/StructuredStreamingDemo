package com.hyr.spark.sql

import com.hyr.spark.sql.common.SparkUtils
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

/** *****************************************************************************
 * @date 2021-01-14 1:32 下午
 * @author: <a href=mailto:huangy>huangyr</a>
 * @Description: 两种RDDs转换Datasets的方式。第二种：通过编程方式将RDD隐式转换为DataFrame
 *               When case classes cannot be defined ahead of time (for example, the structure of records is encoded in a string,
 *               or a text dataset will be parsed and fields will be projected differently for different users), a DataFrame can be created programmatically with three steps.
 *               Create an RDD of Rows from the original RDD;
 *               Create the schema represented by a StructType matching the structure of Rows in the RDD created in Step 1.
 *               Apply the schema to the RDD of Rows via createDataFrame method provided by SparkSession.
 * *****************************************************************************/
object ProgrammaticSchemaExample {

  def main(args: Array[String]): Unit = {
    val spark = SparkUtils.createSparkSession(this.getClass.getName)

    val rdd = spark.sparkContext.textFile("src/main/resources/people.txt")
    // schema is encoded in a string
    val schemaString = "name age"

    val fields = schemaString.split(" ")
      .map(fieldName => StructField(fieldName, StringType, nullable = true))

    val schema = StructType(fields)

    val rowRdd = rdd.map(_.split(","))
      .map(t => Row(t(0), t(1).trim))

    val dataFrame = spark.createDataFrame(rowRdd, schema)

    // 创建临时表视图
    dataFrame.createOrReplaceTempView("people")

    val results = spark.sql("select * from people")
    results.printSchema()

    import spark.implicits._
    results.map(t => "Name: " + t(0)).show(false)
    results.map(t => "Age: " + t.getAs[Int]("age")).show(false)

    spark.stop()
  }

}
