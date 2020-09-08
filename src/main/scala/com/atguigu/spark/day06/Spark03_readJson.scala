package com.atguigu.spark.day06


import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.parsing.json.JSON



/**
 * 读取json格式数据
 */
object Spark03_readJson {
  def main(args: Array[String]): Unit = {
    //2.创建Spark配置文件对象
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark01_CreateRDD_mem")
    //1.创建SparkContext对象
    val sc = new SparkContext(conf)
    val rdd: RDD[String] = sc.textFile("H:\\spark-0105\\input\\test.json")
    val redRdd: RDD[Option[Any]] = rdd.map(JSON.parseFull)

    redRdd.collect().foreach(println)

    //关闭连接
    sc.stop()
  }
}
