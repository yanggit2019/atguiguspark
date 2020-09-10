package com.atguigu.spark.sql.day01

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

//演示RDD&DF&DS之间关系以及转换
object SparkSQL01_Demo {
  def main(args: Array[String]): Unit = {
    //创建SparkConf配置文件对象
    val conf = new SparkConf().setAppName("SparkSQL01_Demo").setMaster("local[*]")
    //创建SPARKSQL 执行的入口对象 SparkSession
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    //导入隐式转换依赖
    //注意:Spark不是包名，也不是类名，是我们创建的入口对象SparkSession的对象名称
    import spark.implicits._
    //读取json文件创建DataFrame
//    val df: DataFrame = spark.read.json("H:\\spark-0105\\input\\test.json")
    //查看df里面的数据
//    df.show()
    //SQL语法风格
//    df.createOrReplaceTempView("user")
//    spark.sql("select * from user").show()
    //DSL风格
//    df.select("name","age").show()
    //RDD----->DataFrame----->DataSet
    //创建RDD
    val rdd: RDD[(Int, String, Int)] = spark.sparkContext.makeRDD(List((1, "banzhang", 20), (2, "jingjing", 18), (3, "wangqiang", 30)))
    //RDD----->DataFrame
    val df: DataFrame = rdd.toDF("id","name","age")
    df.show()
  }
}
