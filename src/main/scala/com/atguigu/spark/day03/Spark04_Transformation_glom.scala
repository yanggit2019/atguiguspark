package com.atguigu.spark.day03

import org.apache.spark
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext, rdd}

//将Rdd一个分区中的元素组合成一个新的数组
object Spark04_Transformation_glom {
  def main(args: Array[String]): Unit = {
    //2.创建Spark配置文件对象
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark04_Transformation_glom")
    //1.创建SparkContext对象
    val sc = new SparkContext(conf)
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6, 7, 8), 2)
    println("-------没有glom之前-------")
    rdd.mapPartitionsWithIndex((index, datas)=>{
      println(index +"------"+datas.mkString(","))
      datas
    }).collect()
    println("-------调用glom之后-------")
    val newRdd: RDD[Array[Int]] = rdd.glom()
    newRdd.mapPartitionsWithIndex((index,datas)=>{
      println(index +"------"+datas.next().mkString(","))
      datas
    }).collect()
//    newRdd.collect().foreach(println)
    //关闭连接
    sc.stop()
  }
}
