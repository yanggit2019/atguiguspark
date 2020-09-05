package com.atguigu.spark.day02

import org.apache.spark
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext, rdd}

object Spark04_Partition_mem {
  def main(args: Array[String]): Unit = {
    //2.创建Spark配置文件对象
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark01_CreateRDD_mem")
    //1.创建SparkContext对象
    val sc = new SparkContext(conf)
    //3.通过集合创建RDD
    //采用默认分区数，集合4个数据，实际输出8个分区---分区中数据分布
//    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
    // 设置分区数为4，集合4个数据，实际输出4个分区---分区中数据分布
//    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
    rdd.saveAsTextFile("H:\\spark-0105\\output")
    

    //关闭连接
    sc.stop()
  }
}
