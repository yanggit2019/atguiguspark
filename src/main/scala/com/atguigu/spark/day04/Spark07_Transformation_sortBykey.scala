package com.atguigu.spark.day04

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

//按照key对Rdd中的元素进行排序
object Spark07_Transformation_sortBykey {
  def main(args: Array[String]): Unit = {
    //2.创建Spark配置文件对象
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark07_Transformation_sortBykey")
    //1.创建SparkContext对象
    val sc = new SparkContext(conf)
    //Rdd本身是没有partitionBy这个算子,通过隐式转换动态给KV类型的Rdd扩展的功能
    //创建rdd
    val rdd: RDD[(Int, String)] = sc.makeRDD(Array((3, "aa"), (6, "cc"), (2, "bb"), (1, "dd")))
    //按照key对rdd中元素进行排序
    val newRdd: RDD[(Int, String)] = rdd.sortByKey()

    newRdd.collect().foreach(println)
    
    //关闭连接
    sc.stop()
  }
}
