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
    val mapRdd: RDD[(String, (Int, Int))] = rdd.map {
      case (name, score) => {
        (name, (score, 1))
      }
    }
    //通过reduceByKey对数据进行聚合(jingjing,(196,2))
    val reduceRdd: RDD[(String, (Int, Int))] = mapRdd.reduceByKey {
      (t1, t2) => {
        (t1._1 + t2._1, t1._2 + t2._2)
      }
    }
    //求出平均值
    val redRdd: RDD[(String, Int)] = reduceRdd.map {
      case (name, (score, count)) => {
        (name, score / count)
      }

    }
    redRdd.collect().foreach(println)
    //关闭连接
    sc.stop()
  }
}
