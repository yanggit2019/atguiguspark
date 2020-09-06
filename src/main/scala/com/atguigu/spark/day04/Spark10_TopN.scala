package com.atguigu.spark.day04

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

//统计每个省份广告点击排名前三的
object Spark10_TopN {
  def main(args: Array[String]): Unit = {
    //2.创建Spark配置文件对象
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark01_CreateRDD_mem")
    //1.创建SparkContext对象
    val sc = new SparkContext(conf)
    //3.读取外部文件 时间戳 省份id 城市id 用户id 广告id
    val logRdd: RDD[String] = sc.textFile("F:\\BaiduNetdiskDownload\\14，Spark\\2.资料\\agent.log")
    //4.对读取到的数据，进行结构转换(省份id-广告id,1)
    val mapRdd: RDD[(String, Int)] = logRdd.map {
      line => {
        //1.用空格对读取的一行字符串进行切分
        val fields: Array[String] = line.split(" ")
        //2.封装为元组结构返回
        (fields(1) + "-" + fields(4), 1)
      }
    }
    //5.对当前省份的每个广告点击次数进行聚合(省份A-广告A,1000)(省份B-广告B,800)
    val reduceRdd: RDD[(String, Int)] = mapRdd.reduceByKey(_ + _)
    //6.再次对结构进行转换，将省作为key(省份A,(广告A,1000)),(省份B,(广告B,800))
    val map1Rdd: RDD[(String, (String, Int))] = reduceRdd.map {
      case (proAndAd, clickCount) => {
        val proAndAdArr: Array[String] = proAndAd.split("-")
        //(身份A,(广告A,clickCount))
        (proAndAdArr(0), (proAndAdArr(1), clickCount))
      }
    }
    //7.按照省份对数据进行分组
    val groupRdd: RDD[(String, Iterable[(String, Int)])] = map1Rdd.groupByKey()
    //8.对每一个省份中的广告点击次数进行降序排序并取得前三名
    val redRDD: RDD[(String, List[(String, Int)])] = groupRdd.mapValues(
      itr => {
        itr.toList.sortWith {
          (left, right) => {
            left._2 > right._2
          }
        }.take(3)
      }
    )
    redRDD.collect().foreach(println)
    //关闭连接
    sc.stop()
  }
}
