package com.atguigu.spark.day06

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_cache {
  def main(args: Array[String]): Unit = {
    //2.创建Spark配置文件对象
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark01_CreateRDD_mem")
    //1.创建SparkContext对象
    val sc = new SparkContext(conf)
    //3.创建rdd
    val rdd: RDD[String] = sc.makeRDD(List("hello bangzhang", "hello jingjing"), 2)
    //4.映射
    val flatMapRdd: RDD[String] = rdd.flatMap(_.split(" "))
    val mapRdd: RDD[(String, Int)] = flatMapRdd.map {
      word => {
        println("------------------")
        (word, 1)
      }
    }
    //添加缓存,对rdd的数据进行缓存
    mapRdd.cache()
    //打印血缘关系
    println(mapRdd.toDebugString)
    //触发行动操作
    mapRdd.collect()
    println("------------------------")
    //打印血缘关系
    println(mapRdd.toDebugString)
    //触发行动操作
    mapRdd.collect()
    //关闭连接
    sc.stop()
  }
}
