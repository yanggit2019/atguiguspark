package com.atguigu.spark.day04

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

//对kV型,根据key对Rdd中的元素进行分区内和分区间进行处理
//aggregateByKey(初始值)(分区内计算,分区间计算)
object Spark04_Transformation_aggregateBykey {
  def main(args: Array[String]): Unit = {
    //2.创建Spark配置文件对象
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark04_Transformation_aggregateBykey")
    //1.创建SparkContext对象
    val sc = new SparkContext(conf)
    //Rdd本身是没有partitionBy这个算子,通过隐式转换动态给KV类型的Rdd扩展的功能
    //创建rdd
    val rdd: RDD[(String,Int)] = sc.makeRDD(List(("a",1), ("b",3), ("a",5),("b",2),("c",4),("c",6),("c",8)),2)
    
    //reduceByKey实现WordCount
//    val resRdd: RDD[(String, Int)] = rdd.reduceByKey(_ + _)
    //使用aggregateByKey实现WordCount
val redRdd: RDD[(String, Int)] = rdd.aggregateByKey(0)(
  (x: Int, y: Int) => x + y,
  (a: Int, b: Int) => a + b
)

    redRdd.collect().foreach(println)
    
    //关闭连接
    sc.stop()
  }
}
