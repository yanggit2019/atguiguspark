package com.atguigu.spark.day02

import org.apache.spark
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext, rdd}

object Spark03_Partition_default {
  def main(args: Array[String]): Unit = {
    //2.创建Spark配置文件对象
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark01_CreateRDD_mem")
    //1.创建SparkContext对象
    val sc = new SparkContext(conf)
    //3.通过集合创建rdd
//    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
    //5.通过读取外部文件创建rdd
    val rdd: RDD[String] = sc.textFile("H:\\spark-0105\\input")
    //4.查看分区效果
     println(rdd.partitions.size)
     rdd.saveAsTextFile("H:\\spark-0105\\output")

    //关闭连接
    sc.stop()
  }
}
