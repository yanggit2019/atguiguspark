package com.atguigu.spark.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

//随机抽选
object Spark09_Transformation_sample {
  def main(args: Array[String]): Unit = {
    //2.创建Spark配置文件对象
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark09_Transformation_sample")
    //1.创建SparkContext对象
    val sc = new SparkContext(conf)
//    val rdd: RDD[String] = sc.makeRDD(List("wangqiao","xiaojing", "hanqi", "chengjiang", "xiaohao"))
//    val newRdd: RDD[String] = rdd.filter(_.contains("xiao"))
//    
//    newRdd.collect().foreach(println)
    val rdd: RDD[Int] = sc.makeRDD(1 to 10)
    //从rdd中随机抽取一些数
    val newRdd: RDD[Int] = rdd.sample(false, 0.6)
    newRdd.collect().foreach(println)
    

    //关闭连接
    sc.stop()
  }
}
