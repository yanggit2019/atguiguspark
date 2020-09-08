package com.atguigu.spark.day06


import org.apache.spark.rdd.RDD
import org.apache.spark.util.{AccumulatorV2, LongAccumulator}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
 * 累加器
 * 分布式共享只读变量，task之间不能读取
 * 自定义累加器，统计出RDD中，所有以“H”开头的单词以及出现次数(word,count)
 */
object Spark07_Accumulator {
  def main(args: Array[String]): Unit = {
    //2.创建Spark配置文件对象
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark07_Accumulator")
    //1.创建SparkContext对象
    val sc = new SparkContext(conf)
    val rdd: RDD[String] = sc.makeRDD(List("Hello", "Hello", "HaHa", "Hello", "HaHa", "Spark", "Spark"))
    //创建累计器对象
    val myAcc = new MyAccumulator
    //注册累计器
    sc.register(myAcc)
    //使用累计器
    rdd.foreach{
      word =>{
        myAcc.add(word)
      }
    }
    //输出累计器结果
    println(myAcc.value)
    //关闭连接
    sc.stop()
  }
}
  /*
  定义一个类，继承AccumulatorV2
  泛型累加器输入和输出数据的类型
   */
class MyAccumulator extends AccumulatorV2[String,mutable.Map[String,Int]]{
    //定义一个集合，集合单词以及出现的次数
    var map = mutable.Map[String,Int]()
    //是否为初始状态
  override def isZero: Boolean = map.isEmpty
  //复制
  override def copy(): AccumulatorV2[String, mutable.Map[String, Int]] = {
    val newAcc = new MyAccumulator
    newAcc.map = this.map
    newAcc
  }
  //重置
  override def reset(): Unit = map.clear()
  //向累计器中添加元素
  override def add(elem: String): Unit = {
    if(elem.startsWith("H")){
      map(elem) = map.getOrElse(elem,0) +1 
    }
    
  }
  //合并
  override def merge(other: AccumulatorV2[String, mutable.Map[String, Int]]): Unit = {
      //当前Excutor的map
    var map1 = map
    //另一个Excutor的map
    var map2 = other.value
     map = map1.foldLeft(map2) {
      //mm表示map2,kv表示map1中的每一个元素
      (mm, kv) => {
        //指定合并规则
        val k: String = kv._1
        val v: Int = kv._2
        //根据map1中的元素key,到map2中获取value
        mm(k) = mm.getOrElse(k, 0) + v
        mm
      }
    }
    }
  //获取累计器的值
  override def value: mutable.Map[String, Int] = {
    map
  }
}