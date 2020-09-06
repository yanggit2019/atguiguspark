package com.atguigu.spark.day04

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

//对kV型,根据key对Rdd中的value进行映射
object Spark09_Transformation_join {
  def main(args: Array[String]): Unit = {
    //2.创建Spark配置文件对象
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark09_Transformation_join")
    //1.创建SparkContext对象
    val sc = new SparkContext(conf)
    val rdd1: RDD[(Int, String)] = sc.makeRDD(List((1, "a"), (2, "b"), (3, "c")))
    val rdd2: RDD[(Int, Int)] = sc.makeRDD(List((1, 4), (2, 5), (4,6)))
    //join算子相当于内连接,将两个RDD中的key相同的数据匹配,如果key匹配不上,那么数据不关联
//    val newRdd: RDD[(Int, (Int, String))] = rdd2.join(rdd1)
//    val newRdd: RDD[(Int, (String, Int))] = rdd1.join(rdd2)
//val newRdd: RDD[(Int, (String, Option[Int]))] = rdd1.leftOuterJoin(rdd2)
    //cogroup 
    val newRdd: RDD[(Int, (Iterable[String], Iterable[Int]))] = rdd1.cogroup(rdd2)
    newRdd.collect().foreach(println)
    //关闭连接
    sc.stop()
  }
}
