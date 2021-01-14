package com.hyr.spark.sql

import com.hyr.spark.sql.common.SparkUtils
import com.hyr.spark.sql.utils.Person
import org.apache.spark.sql.{Encoder, SparkSession}

/** *****************************************************************************
 * @date 2021-01-11 3:04 下午
 * @author: <a href=mailto:huangy>huangyr</a>
 * @Description: 两种RDDs转换Datasets的方式。第一种：通过反射将RDD隐式转换为DataFrame
 *               The Scala interface for Spark SQL supports automatically converting an RDD containing case classes to a DataFrame.
 *               The case class defines the schema of the table. The names of the arguments to the case class are read using reflection
 *               and become the names of the columns. Case classes can also be nested or contain complex types such as Seqs or Arrays.
 *               This RDD can be implicitly converted to a DataFrame and then be registered as a table. Tables can be used in subsequent SQL statements.
 ******************************************************************************/
object InferSchemaExample {

  def main(args: Array[String]): Unit = {
    val spark = SparkUtils.createSparkSession(this.getClass.getName)

    import spark.implicits._
    val rdd = spark.sparkContext.textFile("src/main/resources/people.txt")
    val dataFrame = rdd.map(_.split(","))
      .map(attributes => Person(attributes(0), attributes(1).trim.toInt))
      .toDF()

    dataFrame.createOrReplaceTempView("people")

    val teenagersDF = spark.sql("SELECT name, age FROM people WHERE age BETWEEN 13 AND 19")

    // 通过下标查询
    teenagersDF.map(teenager=>"Name: "+teenager.get(0)).show()

    // 通过字段名查询
    teenagersDF.map(teenager=>"Age: "+teenager.getAs[String]("age")).show()

    // No pre-defined encoders for Dataset[Map[K,V]], define explicitly
    implicit val mapEncoder: Encoder[Map[String, Any]] = org.apache.spark.sql.Encoders.kryo[Map[String, Any]]
    // Primitive types and case classes can be also defined as
    // implicit val stringIntMapEncoder: Encoder[Map[String, Any]] = ExpressionEncoder()

    // row.getValuesMap[T] retrieves multiple columns at once into a Map[String, T]
    teenagersDF.map(teenager => teenager.getValuesMap[Any](List("name", "age"))).collect()
    // Array(Map("name" -> "Justin", "age" -> 19))
    // $example off:schema_inferring$

    spark.stop()
  }

}
