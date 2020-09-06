package com.atguigu.spark.day04

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

//对kV型,根据key对Rdd中的value进行映射
object Spark09_Transformation_join {
  def main(args: Array[String]): Unit = {
    //2.创建Spark配置文件对象
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark09_Transformation_join")
    //1.创建SparkContext对象
    val sc = new SparkContext(conf)
    
    //关闭连接
    sc.stop()
  }
}
