package com.atguigu.spark.sql.day01

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

//自定义函数，在每一个查询的名字前，加问候语
object SparkSQL_UDF {
  def main(args: Array[String]): Unit = {
    //创建SparkConf配置文件对象
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL_UDF")
    //创建sparksession对象
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    
    //创建DF
    val df: DataFrame = spark.read.json("H:\\spark-0105\\input\\test.json")
    //注册自定义函数
    spark.udf.register("addSayHi",(name:String) =>{"nihao:"+name})
    //创建一个临时视图
    df.createOrReplaceTempView("user")
    
    //从临时视图查询语句
    spark.sql("select addSayHi(name),age from user").show
    //释放资源
    spark.stop()
  }
}
