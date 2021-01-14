package com.hyr.spark.sql.utils

/** *****************************************************************************
 * @date 2021-01-14 1:22 下午
 * @author: <a href=mailto:huangy>huangyr</a>
 * @Description: 转换类
 * *****************************************************************************/
case class Person(name: String, age: Long)

case class Employee(name: String, salary: Long)

case class Average(var sum: Long, var count: Long)
