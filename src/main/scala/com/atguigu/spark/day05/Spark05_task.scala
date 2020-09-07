package com.atguigu.spark.day05

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

//job调度 以及Task划分
object Spark05_task {
  def main(args: Array[String]): Unit = {
    //2.创建Spark配置文件对象
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark01_CreateRDD_mem")
    //1.创建SparkContext对象
    val sc = new SparkContext(conf)

    //3. 创建RDD
    val dataRDD: RDD[Int] = sc.makeRDD(List(1,2,3,4,1,2),2)

    //3.1 聚合
    val resultRDD: RDD[(Int, Int)] = dataRDD.map((_,1)).reduceByKey(_+_)

    // Job：一个Action算子就会生成一个Job；
    //3.2 job1打印到控制台
    resultRDD.collect().foreach(println)

    //3.3 job2输出到磁盘
    resultRDD.saveAsTextFile("H:\\spark-0105\\output")

    Thread.sleep(1000000)


    //关闭连接
    sc.stop()
  }
}
