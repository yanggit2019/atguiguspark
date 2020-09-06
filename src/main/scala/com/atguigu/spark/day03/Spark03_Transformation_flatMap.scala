package com.atguigu.spark.day03

import org.apache.spark
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext, rdd}

//对集合中的元素进行扁平化处理
object Spark03_Transformation_flatMap {
  def main(args: Array[String]): Unit = {
    //2.创建Spark配置文件对象
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark03_Transformation_flatMap")
    //1.创建SparkContext对象
    val sc = new SparkContext(conf)
    val rdd: RDD[List[Int]] = sc.makeRDD(List(List(1, 2), List(3, 4), List(5, 6), List(7, 8)), 2)
    //注意：如果匿名函数输入与输出相同，那么不能简化
    val newRdd: RDD[Int] = rdd.flatMap(datas => datas)
    newRdd.collect().foreach(println)
    //关闭连接
    sc.stop()
  }
}
