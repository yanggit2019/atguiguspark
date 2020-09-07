package com.atguigu.spark.day05

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark04_TestLineage {
  def main(args: Array[String]): Unit = {
    //2.创建Spark配置文件对象
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark01_CreateRDD_mem")
    //1.创建SparkContext对象
    val sc = new SparkContext(conf)
    //3.创建Rdd
    val rdd: RDD[String] = sc.makeRDD(List("hello spark", "hello scala", "hello jingjing"), 2)
    //4.查看Rdd血缘关系
    println(rdd.toDebugString)
    println("-------------------")

    val flatMapRdd: RDD[String] = rdd.flatMap(_.split(" "))
    println(flatMapRdd.toDebugString)
    println("------------------")

    val mapRdd: RDD[(String, Int)] = flatMapRdd.map((_, 1))
    println(mapRdd.toDebugString)
    println("----------------------")

    val redRdd: RDD[(String, Int)] = mapRdd.reduceByKey(_ + _)
    println(redRdd.toDebugString)
    println("----------------------")
    
    //关闭连接
    sc.stop()
  }
}
