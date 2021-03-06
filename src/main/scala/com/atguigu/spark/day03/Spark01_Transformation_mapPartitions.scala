package com.atguigu.spark.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_Transformation_mapPartitions {
  def main(args: Array[String]): Unit = {
    //2.创建Spark配置文件对象
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark01_Transformation_mapPartitions")
    //1.创建SparkContext对象
    val sc = new SparkContext(conf)
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)
    //以分区为单位，对Rdd中的元素进行映射
    //一般适用于批处理的操作，比如：将Rdd中的元素插入到数据库中，需要数据的连接，如果每个元素都创建一个连接，可以对每个分区的元素
    //创建一个连接
    val newRdd: RDD[Int] = rdd.mapPartitions(
      _.map(_ * 2)
    )
    newRdd.collect().foreach(println)
    //关闭连接
    sc.stop()
  }
}
