package com.hyr.spark.sql.aggregations

import com.hyr.spark.sql.common.SparkUtils
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

/** *****************************************************************************
 * @date 2021-01-14 2:02 下午
 * @author: <a href=mailto:huangy>huangyr</a>
 * @Description: 用户自定义的无类型的聚合函数
 ******************************************************************************/
object UserDefinedUntypedAggregation {

  def main(args: Array[String]): Unit = {
    val spark = SparkUtils.createSparkSession(this.getClass.getName)

    // 注册UDAF函数
    spark.udf.register("myaverage", MyAverage)

    val df = spark.read.json("src/main/resources/employees.json")
    df.createOrReplaceTempView("employees")
    df.printSchema()
    df.show()

    val result = spark.sql("SELECT myaverage(salary) as average_salary FROM employees")
    result.printSchema()
    result.show()

    spark.stop()
  }

  /**
   * 自定义聚合函数:求平均值
   */
  object MyAverage extends UserDefinedAggregateFunction {

    /**
     * 输入类型 Data types of input arguments of this aggregate function
     */
    override def inputSchema: StructType = {
      StructType(StructField("num", LongType) :: Nil)
    }

    /**
     * 表示聚合缓冲区中值的数据类型  Data types of values in the aggregation buffer
     */
    override def bufferSchema: StructType = StructType(StructField("sum", LongType) :: StructField("count", LongType) :: Nil) // 总和和个数

    /**
     * 返回值的类型
     */
    override def dataType: DataType = DoubleType

    /**
     * 如果此函数是确定性的，则返回true，即，给定相同的输入，始终返回相同的输出。
     */
    override def deterministic: Boolean = true

    /**
     * 初始化缓冲区的类型
     */
    override def initialize(buffer: MutableAggregationBuffer): Unit = {
      buffer(0) = 0L // sum
      buffer(1) = 0L // count
    }

    /**
     * 当收到input输入数据后更新缓冲区
     *
     * @param buffer 缓冲区
     * @param input  输入
     */
    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      if (!input.isNullAt(0)) {
        // 收到数据不为0
        buffer(0) = buffer.getLong(0) + input.getLong(0) // sum
        buffer(1) = buffer.getLong(1) + 1 // count

      }
    }

    /**
     * 多个缓冲区的合并 Merges two aggregation buffers and stores the updated buffer values back to `buffer1`
     */
    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
      buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
    }

    /**
     * 计算最终结果 Calculates the final result
     */
    override def evaluate(buffer: Row): Double = {
      buffer.getLong(0).toDouble / buffer.getLong(1)
    }
  }

}
