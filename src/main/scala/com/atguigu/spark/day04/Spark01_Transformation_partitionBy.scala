package com.atguigu.spark.day04

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
//对kV型的rdd按照key进行重新分区
object Spark01_Transformation_partitionBy {
  def main(args: Array[String]): Unit = {
    //2.创建Spark配置文件对象
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark01_CreateRDD_mem")
    //1.创建SparkContext对象
    val sc = new SparkContext(conf)
    //Rdd本身是没有partitionBy这个算子,通过隐式转换动态给KV类型的Rdd扩展的功能
    //创建rdd
    val rdd: RDD[(Int, String)] = sc.makeRDD(List((1, "aaa"), (2, "bbb"), (3, "ccc")), 3)
    rdd.mapPartitionsWithIndex{
      (index,datas)=>{
        println(index + "-------" +datas.mkString(","))
        datas
      }
    }.collect()
    println("-------------------------------")
    val newRdd: RDD[(Int, String)] = rdd.partitionBy(new HashPartitioner(2))
    newRdd.mapPartitionsWithIndex{
      (index,datas)=>{
        println(index + "-------" +datas.mkString(","))
        datas
      }
    }.collect()
    
    //关闭连接
    sc.stop()
  }
}
