package com.atguigu.spark.day06

import org.apache.spark
import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{Accumulator, SparkConf, SparkContext, rdd}

/**
 * 累加器
 * 分布式共享只读变量，task之间不能读取
 */
object Spark06_Accumulator {
  def main(args: Array[String]): Unit = {
    //2.创建Spark配置文件对象
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark01_CreateRDD_mem")
    //1.创建SparkContext对象
    val sc = new SparkContext(conf)
    //创建一个RDD，并对其进行求和 单值rdd ,直接对数据进行求和，不存在shuffle过程
//    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
//    println(rdd.sum())
//    println(rdd.reduce(_ + _))
    /*
    如果定义一个普通的变量，那么在Driver定义，Excutor会创建变量的副本，算子都是对副本进行操作，Driver端的变量不会更新
     */
    /*val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("a", 2), ("a", 3), ("a", 4)))
    var sum:Int =0
    rdd.foreach{
      case (word,count) =>{
        sum += count
      }
    }
    println(sum)
     */
    
    /*
    如果需要通过Excutor,对Driver端定义的变量进行更新，需要定义为累计器
    累计器和普通的变量相比，会将Excutor端的结果，收集到Driver端进行汇总
     */
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("a", 2), ("a", 3), ("a", 4)))
     //创建累计器
    
//     val sum: Accumulator[Int] = sc.accumulator(10)//过时用法
    val sum: LongAccumulator = sc.longAccumulator("sum")
    rdd.foreach{
      case (word,count) =>{
        //sum += count
        sum.add(count)
      }
    }
    println(sum.value)
    //关闭连接
    sc.stop()
  }
}
