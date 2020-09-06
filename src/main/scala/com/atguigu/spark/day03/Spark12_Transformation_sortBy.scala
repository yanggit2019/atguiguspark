package com.atguigu.spark.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

//重新分区
object Spark12_Transformation_sortBy {
  def main(args: Array[String]): Unit = {
    //2.创建Spark配置文件对象
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark12_Transformation_sortBy")
    //1.创建SparkContext对象
    val sc = new SparkContext(conf)
    val rdd: RDD[Int] = sc.makeRDD(List(1,4,3,2))
    //升序排序
//    val SortedRdd: RDD[Int] = rdd.sortBy(num => num)
    val SortedRdd: RDD[Int] = rdd.sortBy(num => -num)
    SortedRdd.collect().foreach(println)
    
   
    
//    newRdd.collect().foreach(println)
    

    //关闭连接
    sc.stop()
  }
}
