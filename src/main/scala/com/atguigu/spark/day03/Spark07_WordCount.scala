package com.atguigu.spark.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext, rdd}
//使用groupBY完成WordCount
object Spark07_WordCount {
  def main(args: Array[String]): Unit = {
    //2.创建Spark配置文件对象
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark01_CreateRDD_mem")
    //1.创建SparkContext对象
    val sc = new SparkContext(conf)
    //3.创建rdd
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("Hello Scala", 2), ("Hello Spark", 3), ("Hello World", 2)))
    
    //简单版本，方式一
    //对rdd元素扁平映射
//    val flatMap: RDD[String] = rdd.flatMap(_.split(" "))
//    //将映射后的数据进行结构转换，为每个单词计数
//    val mapRdd: RDD[(String, Int)] = flatMap.map((_, 1))
//    //按照key对Rdd中元素进行分组
//    val groupByRdd: RDD[(String, Iterable[(String, Int)])] = mapRdd.groupBy(_._1)
//    //对分组后的元素再次进行映射
//    val resRdd: RDD[(String, Int)] = groupByRdd.map {
//      case (word, datas) => {
//        (word, datas.size)
//      }
//    }
      //第二种实现
//    val flatMap: RDD[String] = rdd.flatMap(_.split(" "))
//    //将rdd中的单词进行分组
//    val groupByRdd: RDD[(String, Iterable[String])] = flatMap.groupBy(word => word)
//    //对分组后的rdd进行映射
//    val resRdd: RDD[(String, Int)] = groupByRdd.map {
//      case (word, datas) => {
//        (word, datas.size)
//      }
//    }
    //复杂版本
    //将原Rdd中字符串以及字符串出现的次数，进行处理，形成一个新的字符串
//    val rdd1: RDD[String] = rdd.map {
//      case (str, count) => {
//        (str + " ") * count
//      }
//    }
//    val flatMapRdd: RDD[String] = rdd1.flatMap(_.split(" "))
//    val groupByRdd: RDD[(String, Iterable[String])] = flatMapRdd.groupBy(datas => datas)
//    val resRdd: RDD[(String, Int)] = groupByRdd.map {
//      case (word, datas) => {
//        (word, datas.size)
//      }
//    }
    //复杂方式实现二
    //对Rdd中的元素进行扁平映射
    val flatMapRdd: RDD[(String, Int)] = rdd.flatMap {
      case (word, count) => {
        //对多个单词组成的字符串进行切割
        val wordArr: Array[String] = word.split(" ")
        wordArr.map(word => (word, count))
      }
    }
    //按照单词对Rdd中的元素进行分组
    val groupByRdd: RDD[(String, Iterable[(String, Int)])] = flatMapRdd.groupBy(_._1)

    val resRdd: RDD[(String, Int)] = groupByRdd.map {
      case (word, datas) => {
        (word, datas.map(_._2).sum)
      }
    }
    resRdd.foreach(println)
    
    //关闭连接
    sc.stop()
  }
}
