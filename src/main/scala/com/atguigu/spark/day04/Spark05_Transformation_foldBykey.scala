package com.atguigu.spark.day04

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

//对kV型,根据key对Rdd中的元素进行分区内和分区间进行处理,分区内和分区间的计算规则是一样
//aggregateByKey(初始值)(分区内计算,分区间计算)
object Spark05_Transformation_foldBykey {
  def main(args: Array[String]): Unit = {
    //2.创建Spark配置文件对象
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark05_Transformation_foldBykey")
    //1.创建SparkContext对象
    val sc = new SparkContext(conf)
    //Rdd本身是没有partitionBy这个算子,通过隐式转换动态给KV类型的Rdd扩展的功能
    //创建rdd
    val rdd: RDD[(String,Int)] = sc.makeRDD(List(("a",1), ("b",3), ("a",5),("b",2),("c",4),("c",6),("c",8)),2)
    //如果分区内和分区间计算规则一样,并且不需要指定初始值,那么优先使用reduceByKey,如果需要指定初始值,优先使用foldByKey.
    val redRdd: RDD[(String, Int)] = rdd.foldByKey(0)(_ + _)
    redRdd.collect().foreach(println)
    
    //关闭连接
    sc.stop()
  }
}
