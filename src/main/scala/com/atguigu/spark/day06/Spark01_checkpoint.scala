package com.atguigu.spark.day06

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_checkpoint {
  def main(args: Array[String]): Unit = {
    //2.创建Spark配置文件对象
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark01_CreateRDD_mem")
    
    //开发环境，应该将检查点目录设置在hdfs上
    //1.创建SparkContext对象
    val sc = new SparkContext(conf)
    //设置检查点目录
    sc.setCheckpointDir("H:\\spark-0105\\output")
    //3.创建rdd
    val rdd: RDD[String] = sc.makeRDD(List("hello bangzhang", "hello jingjing"), 2)
    //4.映射
    val flatMapRdd: RDD[String] = rdd.flatMap(_.split(" "))
    val mapRdd: RDD[(String, Long)] = flatMapRdd.map {
      word => {
        (word, System.currentTimeMillis())
      }
    }
   
    //打印血缘关系
    println(mapRdd.toDebugString)
    //设置检查点
    mapRdd.checkpoint()
    //触发行动操作
    mapRdd.collect().foreach(println)
    println("------------------------")
    //打印血缘关系
    println(mapRdd.toDebugString)
    //触发行动操作
    mapRdd.collect().foreach(println)
    //关闭连接
    sc.stop()
  }
  
}
