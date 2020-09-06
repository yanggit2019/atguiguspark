package com.atguigu.spark.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

//重新分区
object Spark11_Transformation_coalesce {
  def main(args: Array[String]): Unit = {
    //2.创建Spark配置文件对象
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark11_Transformation_coalesce")
    //1.创建SparkContext对象
    val sc = new SparkContext(conf)
//    val rdd: RDD[String] = sc.makeRDD(List("wangqiao","xiaojing", "hanqi", "chengjiang", "xiaohao"))
//    val newRdd: RDD[String] = rdd.filter(_.contains("xiao"))
//    
//    newRdd.collect().foreach(println)
    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4,5,6),3)
    rdd.mapPartitionsWithIndex {
      (index, datas) => {
        println(index + "------>" + datas.mkString(","))
        datas
      }
    }.collect()
    println("---------------------")
    //减少分区
    //注意：默认情况下，使用coalese扩大分区是不起作用，因为底层没有执行shuffle
    //如果扩大分区 使用repartition
//    val newRdd: RDD[Int] = rdd.coalesce(4)
    val newRdd: RDD[Int] = rdd.repartition(4)
     newRdd.mapPartitionsWithIndex{
      (index,datas) =>{
        println(index +"------>"+datas.mkString(","))
        datas
      }
    }.collect()
    
//    newRdd.collect().foreach(println)
    

    //关闭连接
    sc.stop()
  }
}
