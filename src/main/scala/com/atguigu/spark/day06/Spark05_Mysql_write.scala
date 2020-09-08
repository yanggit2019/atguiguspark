package com.atguigu.spark.day06

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.spark.rdd.{JdbcRDD, RDD}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 从mysql中读取数据
 */
object Spark05_Mysql_write {
  def main(args: Array[String]): Unit = {
    //2.创建Spark配置文件对象
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark05_Mysql_write")
    //1.创建SparkContext对象
    val sc = new SparkContext(conf)
    
    var driver = "com.mysql.jdbc.Driver"
    var url = "jdbc:mysql://localhost:3306/test"
    var username = "root"
    var password ="123456"
    val rdd: RDD[(Int, String, Int)] = sc.makeRDD(List((1, "banzhang", 20), (2, "jingjing", 18)))
    rdd.foreach{
      case (id,name,age) =>{
        //注册驱动
        Class.forName(driver)
        ///注册驱动
        Class.forName(driver)
        //获取连接
        val conn: Connection = DriverManager.getConnection(url, username, password)
        //声明数据库操作的SQL语句
        val sql:String ="insert into spark_test(id,name,age) values(?,?,?)"
        //创建数据库操作对象PrepareStatement
        val ps: PreparedStatement = conn.prepareStatement(sql)
        //给参数赋值
        ps.setInt(1,id)
        ps.setString(2,name)
        ps.setInt(3,age)
        //执行SQL
        ps.executeUpdate()
        //关闭连接
        ps.close()
        conn.close()
      }
    }
    
    
    //关闭连接
    sc.stop()
  }
}
