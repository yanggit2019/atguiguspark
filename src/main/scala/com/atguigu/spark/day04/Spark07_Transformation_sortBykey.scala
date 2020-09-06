package com.atguigu.spark.day04

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

//按照key对Rdd中的元素进行排序
object Spark07_Transformation_sortBykey {
  def main(args: Array[String]): Unit = {
    //2.创建Spark配置文件对象
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark07_Transformation_sortBykey")
    //1.创建SparkContext对象
    val sc = new SparkContext(conf)
    //Rdd本身是没有partitionBy这个算子,通过隐式转换动态给KV类型的Rdd扩展的功能
//    //创建rdd
//    val rdd: RDD[(Int, String)] = sc.makeRDD(Array((3, "aa"), (6, "cc"), (2, "bb"), (1, "dd")))
//    //按照key对rdd中元素进行排序
//    val newRdd: RDD[(Int, String)] = rdd.sortByKey(false)
    //如果key为自定义类型,要求必须混入Ordered特质
val stuList = List((new Student("jingjing", 18), 1),
  (new Student("bangzhang", 18), 1),
  (new Student("jingjing", 19), 1),
  (new Student("luoxiang", 18), 1),
  (new Student("jingjing", 20), 1)
)
    val rdd: RDD[(Student, Int)] = sc.makeRDD(stuList)
    rdd.sortByKey().collect().foreach(println)
    
    
    //关闭连接
    sc.stop()
  }
}
class Student(var name:String,var age:Int) extends Ordered[Student] with Serializable {
  //指定比较规则
  override def compare(that: Student): Int = {
    //先按照名称排序,如果名称相同的话,再按年龄排序
    val res: Int = this.name.compareTo(that.name)
    res
  }

  override def toString: String = s"Student($name,$age)"
}