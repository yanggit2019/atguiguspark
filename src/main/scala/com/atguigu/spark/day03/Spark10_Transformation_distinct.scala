package com.atguigu.spark.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

//去重
object Spark10_Transformation_distinct {
  def main(args: Array[String]): Unit = {
    //2.创建Spark配置文件对象
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark10_Transformation_distinct")
    //1.创建SparkContext对象
    val sc = new SparkContext(conf)
//    val rdd: RDD[String] = sc.makeRDD(List("wangqiao","xiaojing", "hanqi", "chengjiang", "xiaohao"))
//    val newRdd: RDD[String] = rdd.filter(_.contains("xiao"))
//    
//    newRdd.collect().foreach(println)
    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4,5,5,4,3,2,3,6),5)
    val newRdd: RDD[Int] = rdd.distinct(2)
    
    newRdd.collect().foreach(println)
    

    //关闭连接
    sc.stop()
  }
}
