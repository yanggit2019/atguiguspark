package com.atguigu.spark.day06

import java.sql.DriverManager

import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 从mysql中读取数据
 */
object Spark04_Mysql_read {
  def main(args: Array[String]): Unit = {
    //2.创建Spark配置文件对象
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark01_CreateRDD_mem")
    //1.创建SparkContext对象
    val sc = new SparkContext(conf)
    /**
     * sc: SparkContext,  spark程序执行入口，上下文对象
     * getConnection: () => Connection,  获取数据库连接
     * sql: String,  执行sql语句
     * lowerBound: Long, 查询起始位
     * upperBound: Long, 查询结束位
     * numPartitions: Int,  分区数
     * mapRow: (ResultSet) => T = JdbcRDD.resultSetToObjectArray _)  对结果集进行处理
     * 注册驱动
     * 获取连接
     * 创建数据库操作对象PrepareStatement
     * 执行SQL
     * 处理结果集
     * 关闭连接
     */
    //创建rdd
    /**
     * 数据库连接四要素
     * 
     */
    var driver = "com.mysql.jdbc.Driver"
    var url = "jdbc:mysql://localhost:3306/hainiu_test"
    var username = "root"
    var password ="123456"
    
    var sql ="select * from hainiu_web_seed_internally where id >= ? and id <= ? "
    val resRdd = new JdbcRDD(
      sc,
      () => {
        //注册驱动
        Class.forName(driver)
        //获取连接
        DriverManager.getConnection(url, username, password)
      },
      sql,
      300,
      500,
      2,
      rs => (rs.getInt(1), rs.getString(2), rs.getString(3))
    )
    resRdd.collect().foreach(println)
    //关闭连接
    sc.stop()
  }
}
