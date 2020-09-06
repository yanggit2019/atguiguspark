package com.atguigu.spark.day04

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, Partitioner, SparkConf, SparkContext}
//对kV型的rdd按照key进行重新分区
object Spark02_Transformation_reduceBykey {
  def main(args: Array[String]): Unit = {
    //2.创建Spark配置文件对象
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark03_Transformation_reduceBykey")
    //1.创建SparkContext对象
    val sc = new SparkContext(conf)
    //Rdd本身是没有partitionBy这个算子,通过隐式转换动态给KV类型的Rdd扩展的功能
    //创建rdd
    val rdd: RDD[(String,Int)] = sc.makeRDD(List(("a",1), ("b",3), ("a",5),("b",2)))
    val resRdd: RDD[(String,Int )] = rdd.reduceByKey(_ + _)
    resRdd.collect().foreach(println)
    
    //关闭连接
    sc.stop()
  }
}
