package com.atguigu.spark.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

//按照指定的规则对rdd的元素进行分组
object Spark05_Transformation_groupBy {
  def main(args: Array[String]): Unit = {
    //2.创建Spark配置文件对象
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark05_Transformation_groupBy")
    //1.创建SparkContext对象
    val sc = new SparkContext(conf)
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6, 7, 8), 3)
    println("-------执行groupBy之前------")
    rdd.mapPartitionsWithIndex((index,datas)=>{
      println(index+"------->"+datas.mkString(","))
      datas
    }).collect()
    val newRdd: RDD[(Int, Iterable[Int])] = rdd.groupBy(_ % 2)
    println("----执行groupBy之后-------")
    newRdd.mapPartitionsWithIndex((index,datas)=>{
      println(index+"------->"+datas.mkString(","))
      datas
    }).collect()
//    newRdd.collect().foreach(println)
    //关闭连接
    sc.stop()
  }
}
