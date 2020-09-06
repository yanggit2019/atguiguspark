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
    val groupRdd: RDD[(String, Iterable[Int])] = rdd.groupByKey()
    val redRdd: RDD[(String, Int)] = groupRdd.map {
      case (name, scoreSeq) => {
        (name, scoreSeq.sum / scoreSeq.size)
      }
    }
    redRdd.collect().foreach(println)
    
    //关闭连接
    sc.stop()
  }
}
