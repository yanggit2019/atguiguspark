package com.atguigu.spark.day05

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

//序列化
object Spark02_TestSerializable {
  def main(args: Array[String]): Unit = {
    //2.创建Spark配置文件对象
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark01_CreateRDD_mem")
    //1.创建SparkContext对象
    val sc = new SparkContext(conf)
//    3.创建User对象
    val user1 = new User
    user1.name = "zhangsan" 
    val user2 = new User
    user2.name = "lisi"
    val rdd: RDD[User] = sc.makeRDD(List(user1, user2))
    rdd.foreach(println)
    //关闭连接
    sc.stop()
  }
}
class User{
  var name:String = _

  override def toString: String = s"User(${name})"
}