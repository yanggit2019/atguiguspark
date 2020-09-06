package com.atguigu.spark.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

//双value类型
object Spark13_Transformation_doubleValue {
  def main(args: Array[String]): Unit = {
    //2.创建Spark配置文件对象
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark13_Transformation_doubleValue")
    //1.创建SparkContext对象
    val sc = new SparkContext(conf)
    val rdd1: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
    val rdd2: RDD[Int] = sc.makeRDD(List(4,5,6,7))
    //合集
    val newRdd: RDD[Int] = rdd1.union(rdd2)
    newRdd.collect().foreach(println)
    
  
    
   
    

    

    //关闭连接
    sc.stop()
  }
}
