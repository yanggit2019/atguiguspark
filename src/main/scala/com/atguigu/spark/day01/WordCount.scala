package com.atguigu.spark.day01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {
    //2.创建SparkConf配置文件
    val conf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    //1.创建SparkContext对象
    val sc = new SparkContext(conf)
    //3.读取外部文件
    val textRdd: RDD[String] = sc.textFile("H:\\spark-0105\\input")
    //4.打印输出一下
    val flatMapRdd: RDD[String] = textRdd.flatMap(_.split(" "))
    //5. 对数据集中的内容进行结构的转换----计数
    val mapRdd: RDD[(String, Int)] = flatMapRdd.map((_, 1))
    //6.对相同的单词 出现的次数进行汇总
    val reduceRdd: RDD[(String, Int)] = mapRdd.reduceByKey(_ + _)
    //7.将执行的结果进行收集
    val res: Array[(String, Int)] = reduceRdd.collect()
    
//    res.foreach(println)
    //8.释放资源
    sc.stop()
  }
  
}
