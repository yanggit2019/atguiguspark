package com.atguigu.spark.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
//按照指定的过滤规则，对rdd中的元素进行过滤
object Spark08_Transformation_filter {
  def main(args: Array[String]): Unit = {
    //2.创建Spark配置文件对象
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark08_Transformation_filter")
    //1.创建SparkContext对象
    val sc = new SparkContext(conf)
//    val rdd: RDD[String] = sc.makeRDD(List("wangqiao","xiaojing", "hanqi", "chengjiang", "xiaohao"))
//    val newRdd: RDD[String] = rdd.filter(_.contains("xiao"))
//    
//    newRdd.collect().foreach(println)
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6, 7, 8, 9), 2)
    val newRdd: RDD[Int] = rdd.filter(_ % 2 != 0)
    newRdd.collect().foreach(println)
    

    //关闭连接
    sc.stop()
  }
}
