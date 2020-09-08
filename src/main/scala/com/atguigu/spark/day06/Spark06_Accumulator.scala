package com.atguigu.spark.day06

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 累加器
 */
object Spark06_Accumulator {
  def main(args: Array[String]): Unit = {
    //2.创建Spark配置文件对象
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark01_CreateRDD_mem")
    //1.创建SparkContext对象
    val sc = new SparkContext(conf)
    //创建一个RDD，并对其进行求和 单值rdd ,直接对数据进行求和，不存在shuffle过程
//    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
//    println(rdd.sum())
//    println(rdd.reduce(_ + _))
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("a", 2), ("a", 3), ("a", 4)))
    var sum:Int =0
    rdd.foreach{
      case (word,count) =>{
        sum += count
      }
    }
    println(sum)
    //关闭连接
    sc.stop()
  }
}
