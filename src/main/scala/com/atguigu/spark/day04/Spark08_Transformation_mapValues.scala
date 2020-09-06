package com.atguigu.spark.day04

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

//对kV型,根据key对Rdd中的value进行映射
object Spark08_Transformation_mapValues {
  def main(args: Array[String]): Unit = {
    //2.创建Spark配置文件对象
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark06_Transformation_combineBykey")
    //1.创建SparkContext对象
    val sc = new SparkContext(conf)
    val rdd: RDD[(Int, String)] = sc.makeRDD(List((1, "a"), (1, "d"), (2, "b"), (3, "c")))
    val newRdd: RDD[(Int, String)] = rdd.mapValues("|||" + _)
    newRdd.collect().foreach(println)
    
    //关闭连接
    sc.stop()
  }
}
