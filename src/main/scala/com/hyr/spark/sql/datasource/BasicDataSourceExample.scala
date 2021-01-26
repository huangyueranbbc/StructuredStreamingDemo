package com.hyr.spark.sql.datasource

import com.hyr.spark.sql.common.SparkUtils

/** *****************************************************************************
 *
 * @date 2021-01-14 2:51 下午
 * @author: <a href=mailto:huangy>huangyr</a>
 * @Description:
 * *****************************************************************************/
object BasicDataSourceExample {

  def main(args: Array[String]): Unit = {
    val spark = SparkUtils.createSparkSession(this.getClass.getName)

    // $example on:generic_load_save_functions$
    val usersDF = spark.read.load("src/main/resources/users.parquet")
    usersDF.select("name", "favorite_color").write.save("namesAndFavColors.parquet")
    val peopleDF = spark.read.format("json").load("src/main/resources/people.json")
    peopleDF.select("name", "age").write.format("parquet").save("namesAndAges.parquet")
    val peopleDFCsv = spark.read.format("csv")
      .option("sep", ";")
      .option("inferSchema", "true")
      .option("header", "true")
      .load("src/main/resources/people.csv")
    // $example off:manual_load_options_csv$
    // $example on:manual_save_options_orc$
    usersDF.write.format("orc")
      .option("orc.bloom.filter.columns", "favorite_color")
      .option("orc.dictionary.key.threshold", "1.0")
      .save("users_with_options.orc")
    // $example off:manual_save_options_orc$

    // $example on:direct_sql$
    val sqlDF = spark.sql("SELECT * FROM parquet.`src/main/resources/users.parquet`")
    // $example off:direct_sql$
    // $example on:write_sorting_and_bucketing$
    peopleDF.write.bucketBy(42, "name").sortBy("age").saveAsTable("people_bucketed")
    // $example off:write_sorting_and_bucketing$
    // $example on:write_partitioning$
    usersDF.write.partitionBy("favorite_color").format("parquet").save("namesPartByColor.parquet")

    // $example off:write_partitioning$
    // $example on:write_partition_and_bucket$
    usersDF
      .write
      // 分区
      .partitionBy("favorite_color")
      // 分桶
      .bucketBy(42, "name")
      .saveAsTable("users_partitioned_bucketed")
    // $example off:write_partition_and_bucket$

    spark.sql("show create table users_partitioned_bucketed").show(false)

    spark.sql("DROP TABLE IF EXISTS people_bucketed")
    spark.sql("DROP TABLE IF EXISTS users_partitioned_bucketed")
  }

}
