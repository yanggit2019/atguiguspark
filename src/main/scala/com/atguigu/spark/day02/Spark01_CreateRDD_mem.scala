package com.atguigu.spark.day02

import org.apache.spark
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext, rdd}

object Spark01_CreateRDD_mem {
  def main(args: Array[String]): Unit = {
    //2.创建Spark配置文件对象
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark01_CreateRDD_mem")
    //1.创建SparkContext对象
    val sc = new SparkContext(conf)
    //3.创建一个集合对象
    val list: List[Int] = List(1, 2, 3, 4)
    //4.根据集合创建Rdd
//    val rdd: RDD[Int] = sc.parallelize(list)
    //5.方式二:根据集合创建Rdd
    val rdd: RDD[Int] = sc.makeRDD(list)
    rdd.collect().foreach(println)

    //关闭连接
    sc.stop()
  }
}
