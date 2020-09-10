package com.atguigu.spark.sql.day01

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

//演示RDD&DF&DS之间关系以及转换
object SparkSQL01_Demo {
  def main(args: Array[String]): Unit = {
    //创建SparkConf配置文件对象
    val conf = new SparkConf().setAppName("SparkSQL01_Demo").setMaster("local[*]")
    //创建SPARKSQL 执行的入口对象 SparkSession
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    //读取json文件创建DataFrame
    val df: DataFrame = spark.read.json("H:\\spark-0105\\input\\test.json")
    //查看df里面的数据
    df.show()
    
  }
}
