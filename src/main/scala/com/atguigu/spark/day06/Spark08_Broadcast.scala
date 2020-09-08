package com.atguigu.spark.day06

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 广播变量：分布式共享只读变量
 */
object Spark08_Broadcast {
  def main(args: Array[String]): Unit = {
    //2.创建Spark配置文件对象
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark01_CreateRDD_mem")
    //1.创建SparkContext对象
    val sc = new SparkContext(conf)
    //想实现类似join的效果 (a,(1,4)),(b,(2,5)),(c,(3,6))
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("b", 2), ("c", 3)))
    val list: List[(String, Int)] = List(("a", 4), ("b", 5), ("c", 6))
    //创建一个广播变量
    val broadcastList: Broadcast[List[(String, Int)]] = sc.broadcast(list)


    val resRdd: RDD[(String, (Int, Any))] = rdd.map {
      case (k1, v1) => {
        var v3 = 0
        //        for ((k2, v2) <- list) {
        for ((k2, v2) <- broadcastList.value) {
          if (k1 == k2) {
            v3 = v2
          }
        }
        (k1, (v1, v3))
      }
    }

    resRdd.collect().foreach(println)
    //关闭连接
    sc.stop()
  }
}
