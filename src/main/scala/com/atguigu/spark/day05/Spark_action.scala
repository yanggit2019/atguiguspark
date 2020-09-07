package com.atguigu.spark.day05

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark_action {
  def main(args: Array[String]): Unit = {
    //2.创建Spark配置文件对象
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark01_CreateRDD_mem")
    //1.创建SparkContext对象
    val sc = new SparkContext(conf)
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
    //reduce
//    val res: Int = rdd.reduce(_ + _)
//    print(res)
//    //collect
//    val ints: Array[Int] = rdd.collect()
//    ints.foreach(println)
    //foreach
//    rdd.foreach(println)
    //count 获取rdd中元素的个数
//val res: Long = rdd.count()
//    println(res)
    // first 返回rdd中第一个元素
//val res: Int = rdd.first()
//    println(res)
    //take 返回rdd前n个元素组成数组
val res: Array[Int] = rdd.take(3)
    res.foreach(println)
    //关闭连接
    sc.stop()
  }
}
