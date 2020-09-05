package com.atguigu.spark.day02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_CreateRDD_file {
  def main(args: Array[String]): Unit = {
    //2.创建Spark配置文件对象
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark01_CreateRDD_mem")
    //1.创建SparkContext对象
    val sc = new SparkContext(conf)

    //3.从本地文件读取数据，创建rdd
//    val rdd: RDD[String] = sc.textFile("H:\\spark-0105\\input")
//    rdd.collect().foreach(println)
    //4.从hdfs上读取数据，创建rdd
    val rdd: RDD[String] = sc.textFile("hdfs://192.168.88.199:9000:/user/liuyiyang21/spark/output")
    rdd.collect().foreach(println)
    //关闭连接
    sc.stop()
  }
}
