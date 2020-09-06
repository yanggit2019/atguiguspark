package com.atguigu.spark.day04

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

//对kV型,根据key对Rdd中的元素进行分区内和分区间进行处理,分区内和分区间的计算规则是一样
//aggregateByKey(初始值)(分区内计算,分区间计算)
object Spark06_Transformation_combineBykey {
  def main(args: Array[String]): Unit = {
    //2.创建Spark配置文件对象
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark06_Transformation_combineBykey")
    //1.创建SparkContext对象
    val sc = new SparkContext(conf)
    //Rdd本身是没有partitionBy这个算子,通过隐式转换动态给KV类型的Rdd扩展的功能
    //创建rdd
    val rdd: RDD[(String,Int)] = sc.makeRDD(List(("jingjing",90),("jiafeng",60),("jingjing",96),("jiafeng",62),("jingjing",100),("jiafeng",50)))
    //求出每个学生的平均成绩
    //方案一 
//    val groupRdd: RDD[(String, Iterable[Int])] = rdd.groupByKey()
//    val redRdd: RDD[(String, Int)] = groupRdd.map {
//      case (name, scoreSeq) => {
//        (name, scoreSeq.sum / scoreSeq.size)
//      }
//    }
//    redRdd.collect().foreach(println)
    //如果某个组数据量比较大,会造成单点压力
    //方案2 使用reduceByKey (name,score)===>(name,(score,1))
    //对Rdd中的数据进行结构的转换
//    
    //方案三 通过combineByKey来实现
    // createCombiner: V => C,从RDD中当前key取出第一个Value做一个初始化
    //      mergeValue: (C, V) => C,分区内计算规则,主要在分区内进行,将当前分区的value值,合并到初始化得到的c上面
    //      mergeCombiners: (C, C) => C 分区间计算规则
    val combineRdd: RDD[(String, (Int, Int))] = rdd.combineByKey(
      (_, 1),
      (tup1: (Int, Int), v) => {
        (tup1._1 + v, tup1._2 + 1)
      },
      (tup2: (Int, Int), tup3: (Int, Int)) => {
        (tup2._1 + tup3._1, tup2._2 + tup3._2)
      }
    )
    val resRdd: RDD[(String, Int)] = combineRdd.map {
      case (name, (score, count)) => {
        (name, score / count)
      }
    }
    resRdd.collect().foreach(println)
    
    //关闭连接
    sc.stop()
  }
}
