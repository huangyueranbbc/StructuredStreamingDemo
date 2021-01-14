package com.hyr.structured.streaming.sink

import java.sql.Connection
import java.util.Properties

import com.alibaba.druid.pool.DruidDataSourceFactory
import org.apache.spark.sql.{ForeachWriter, Row}

/** *****************************************************************************
 *
 * @date 2020-12-31 10:07 上午
 * @author: <a href=mailto:huangyr>huangyr</a>
 * @Description: MySQL Sink 将数据写入MySQL
 *
 *               create table wordcount
 *               (
 *               id         int(100) primary key auto_increment comment '主键',
 *               data_key   varchar(10000) comment 'kafka的key',
 *               data_value varchar(10000) comment 'kafka的value',
 *               data_partition varchar(300)  comment 'kafka的partition',
 *               data_offset varchar(300)  comment 'kafka的offset',
 *               data_topic varchar(300)  comment 'kafka的topic',
 *               data_timestamp varchar(300)  comment 'kafka的timestamp'
 *               )DEFAULT CHARACTER SET = utf8;
 ******************************************************************************/
class MysqlWriter extends ForeachWriter[Row] {
  private var connection: Connection = _

  override def open(partitionId: Long, epochId: Long): Boolean = {
    //数据源配置
    val properties = new Properties()
    //通过当前类的class对象获取资源文件
    val file = classOf[MysqlWriter].getResourceAsStream("/druid.properties")
    properties.load(file)
    //返回的是DataSource，不是DruidDataSource
    val dataSource = DruidDataSourceFactory.createDataSource(properties)
    //获取连接
    connection = dataSource.getConnection
    true
  }

  override def process(value: Row): Unit = {
    println("key:" + value.getAs[String]("key"))
    println("value:" + value.getAs[String]("value"))
    val prepareStatement = connection.prepareStatement("insert into wordcount(data_key,data_value,data_partition,data_offset,data_topic,data_timestamp) value(?,?,?,?,?,?)")
    prepareStatement.setString(1, s"${value.getAs[String]("key")}")
    prepareStatement.setString(2, s"${value.getAs[String]("value")}")
    prepareStatement.setString(3, s"${value.getAs[String]("partition")}")
    prepareStatement.setString(4, s"${value.getAs[String]("offset")}")
    prepareStatement.setString(5, s"${value.getAs[String]("topic")}")
    prepareStatement.setString(6, s"${value.getAs[String]("timestamp")}")
    prepareStatement.addBatch()
    // 如果是查询的话返回true，如果是更新或插入的话就返回false；
    val bool = prepareStatement.execute()
    println("insert result:" + bool)
  }

  override def close(errorOrNull: Throwable): Unit = {
    if (connection != null && !connection.isClosed) {
      connection.close()
    }
  }
}
