package com.atguigu.spark.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark06_Transformation_map {
  def main(args: Array[String]): Unit = {
    //2.创建Spark配置文件对象
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark01_CreateRDD_mem")
    //1.创建SparkContext对象
    val sc = new SparkContext(conf)
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)
    println("原分区个数:"+rdd.partitions.size)
    val newRdd: RDD[Int] = rdd.map(_ * 2)
    println("新分区个数"+newRdd.partitions.size)
    newRdd.collect().foreach(println)

    //关闭连接
    sc.stop()
  }
}
