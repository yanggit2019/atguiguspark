package com.atguigu.spark.sql.day01

import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

//自定义聚合函数，求平均年龄----RDD累计器实现
object SparkSQL_Accumulator {
  def main(args: Array[String]): Unit = {
    //2.创建Spark配置文件对象
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL_RDD")
    //1.创建SparkContext对象
    val sc = new SparkContext(conf)
    //3.创建一个Rdd
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("jingjing", 20), ("banzhang", 40), ("xucong", 30)))
    //创建累计器对象
    val myAcc = new MyAccumulator
    //注册累加器
    sc.register(myAcc)
    //使用累加器
    rdd.foreach{
      case (name,age) =>{
        myAcc.add(age)
      }
    }
    //获取累计器的值
    println(myAcc.value)
    //关闭连接
    sc.stop()
  }
}
class MyAccumulator extends AccumulatorV2[Int,Double] {
  var ageSum:Int =0
  var countSum:Int =0
  override def isZero: Boolean = {
    ageSum == 0 && countSum ==0
  }

  override def copy(): AccumulatorV2[Int, Double] = {
    val newMyAcc = new MyAccumulator
    newMyAcc.countSum = this.countSum
    newMyAcc.countSum = this.ageSum
    newMyAcc
  }

  override def reset(): Unit = {
    ageSum =0
    countSum =0
  }

  override def add(age: Int): Unit = {
    ageSum += age
    countSum += 1
  }

  override def merge(other: AccumulatorV2[Int, Double]): Unit = {
    other match {
      case mc:MyAccumulator =>{
        this.ageSum += mc.ageSum
        this.countSum += mc.countSum
      }
      case _ =>
    }
  }

  override def value: Double = {
    ageSum/countSum
  }
}