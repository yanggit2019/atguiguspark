package com.atguigu.spark.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
//以分区为单位，对Rdd中的元素进行映射，并且带分区编号
object Spark02_Transformation_mapPartitionsWithIndex {
  def main(args: Array[String]): Unit = {
    //2.创建Spark配置文件对象
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark02_Transformation_mapPartitionsWithIndex")
    //1.创建SparkContext对象
    val sc = new SparkContext(conf)
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)
//    val newRdd: RDD[Int] = rdd.map(_ * 2)
//    val newRdd: RDD[Int] = rdd.mapPartitions(datas => {
//      datas.map(_ * 2)
//    })
    
    val newRdd: RDD[(Int, Int)] = rdd.mapPartitionsWithIndex((index, datas) => {
      datas.map((index, _))
    })
    newRdd.collect().foreach(println)
    //关闭连接
    sc.stop()
  }
}
