package com.atguigu.spark.sql.day01

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/*
自定义UDAF(弱类型  主要应用在SQL风格的DF查询)
 */
object SparkSQL_UDAF {
  def main(args: Array[String]): Unit = {
    //创建配置文件对象
    val conf: SparkConf = new SparkConf().setAppName("SparkSQL_UDAF").setMaster("local[*]")
    
    //创建SparkSession对象
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    
    //读取JSON文件，创建UDF
    val df: DataFrame = spark.read.json("H:\\spark-0105\\input\\test.json")
    
    //创建临时视图
    df.createOrReplaceTempView("user")
    
    //使用聚合函数来进行查询
    spark.sql("select avg(age) from user").show()
    
    //关闭连接
    spark.stop()
    
  }
}
