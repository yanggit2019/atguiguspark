package com.atguigu.spark.day02

import org.apache.spark
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext, rdd}

object Spark05_Partition_file {
  def main(args: Array[String]): Unit = {
    //2.创建Spark配置文件对象
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark05_Partition_file")
    //1.创建SparkContext对象
    val sc = new SparkContext(conf)

    //3.从本地文件读取数据，创建rdd
    //输入数据1 2 3 4 采用默认分区方式，最终分区数2
//    val rdd: RDD[String] = sc.textFile("H:\\spark-0105\\input\\2.txt")
    //输入数据1 2 3 4 指定分区数，minPartitions设置为3，最终分区数为4
//    val rdd: RDD[String] = sc.textFile("H:\\spark-0105\\input\\2.txt",3)
//    输入数据123456,minPartition设置为3，最终分区数3
    val rdd: RDD[String] = sc.textFile("H:\\spark-0105\\input\\2.txt",3)
    //输入数据123 4567 ,minPartition设置为3,最终分区数3
    rdd.saveAsTextFile("H:\\spark-0105\\output")
//    rdd.collect().foreach(println)
    //关闭连接
    sc.stop()
  }
}
