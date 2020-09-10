package com.atguigu.spark.sql.day01

import org.apache
import org.apache.spark
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}

//自定义聚合函数，求平均年龄----RDD算子方式实现
object SparkSQL_RDD {
  def main(args: Array[String]): Unit = {
    //2.创建Spark配置文件对象
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL_RDD")
    //1.创建SparkContext对象
    val sc = new SparkContext(conf)
    //3.创建一个Rdd
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("jingjing", 20), ("banzhang", 40), ("xucong", 30)))
    val mapRdd: RDD[(Int, Int)] = rdd.map {
      case (name, age) => {
        (age, 1)
      }
    }
    //对年龄以及总人数来进行聚合 (agesum,countsum)
    val res: (Int, Int) = mapRdd.reduce {
      (t1, t2) => {
        (t1._1 + t2._1, t1._2 + t2._2)
      }
    }
    println(res._1/res._2)
    //关闭连接
    sc.stop()
  }
}
