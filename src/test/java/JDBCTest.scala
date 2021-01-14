import java.util.Properties

import com.alibaba.druid.pool.DruidDataSourceFactory
import com.hyr.structured.streaming.sink.MysqlWriter

/** *****************************************************************************
 * @date 2020-12-31 10:24 上午
 * @author: <a href=mailto:huangyr>huangyr</a>
 * @Description:
 * *****************************************************************************/
object JDBCTest {

  def main(args: Array[String]): Unit = {
    //数据源配置
    val properties = new Properties()
    //通过当前类的class对象获取资源文件
    val file = classOf[MysqlWriter].getResourceAsStream("/druid.properties")
    properties.load(file)
    //返回的是DataSource，不是DruidDataSource
    val dataSource = DruidDataSourceFactory.createDataSource(properties)
    //获取连接
    val connection = dataSource.getConnection
    println(connection)
    val set = connection.createStatement().executeQuery("show databases")
    while (set.next()){
      println(set.getString(1))
    }
  }

}
