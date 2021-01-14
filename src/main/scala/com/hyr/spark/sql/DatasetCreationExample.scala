package com.hyr.spark.sql

import com.hyr.spark.sql.common.SparkUtils
import com.hyr.spark.sql.utils.Person
import org.apache.spark.sql.SparkSession

/** *****************************************************************************
 * @date 2021-01-11 2:49 下午
 * @author: <a href=mailto:huangy>huangyr</a>
 * @Description: 数据集DataSets基本操作 http://spark.apache.org/docs/2.4.0/sql-programming-guide.html
 *               Datasets are similar to RDDs, however, instead of using Java serialization or Kryo they
 *               use a specialized Encoder to serialize the objects for processing or transmitting over the network.
 *               While both encoders and standard serialization are responsible for turning an object into bytes,
 *               encoders are code generated dynamically and use a format that allows Spark to perform many operations like filtering,
 *               sorting and hashing without deserializing the bytes back into an object.
 ******************************************************************************/
object DatasetCreationExample {

  def main(args: Array[String]): Unit = {
    val spark = SparkUtils.createSparkSession(this.getClass.getName)

    import spark.implicits._
    // $example on:create_ds$
    // Encoders are created for case classes
    // Seq是一个特征, 代表可以保证不变的索引序列。你可以使用元素索引来访问元素。它保持元素的插入顺序。
    val caseClassDS = Seq(Person("Andy", 32)).toDS()
    caseClassDS.show()

    // Encoders for most common types are automatically provided by importing spark.implicits._
    val primitiveDS = Seq(1, 2, 3).toDS()
    val ints = primitiveDS.map(_ + 1).collect() // Returns: Array(2, 3, 4)
    ints.foreach(println)

    // 将DataFrame转换为DataSet DataFrames can be converted to a Dataset by providing a class. Mapping will be done by name
    val personDS = spark.read.json("src/main/resources/people.json").as[Person]
    personDS.printSchema()
    personDS.show(false)

    spark.stop()
  }

}
