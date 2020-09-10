package com.atguigu.spark.sql.day01

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.{Aggregator, MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, IntegerType, LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Encoders, Row, SparkSession, TypedColumn}

/*
自定义UDAF(强类型  主要应用在DSL风格的DS查询)
 */
object SparkSQL02_UDAF {
  def main(args: Array[String]): Unit = {
    //创建配置文件对象
    val conf: SparkConf = new SparkConf().setAppName("SparkSQL02_UDAF").setMaster("local[*]")
    
    //创建SparkSession对象
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._
    //创建DataFrame
    val df: DataFrame = spark.read.json("H:\\spark-0105\\input\\test.json")

    /**
     * 注意：如果是自定义UDAF的强类型，没有办法应用SQL风格DF的查询
     * //    //创建自定义函数对象
     * //    val myAvgNew = new MyAvgNew
     * //    //注册自定义函数
     * //    spark.udf.register("myAvgNew",myAvgNew)
     * //    //创建临时视图
     * //    df.createOrReplaceTempView("user")
     * //    //使用聚合函数进行查询
     * //    spark.sql("select myAvgNew(age) from user").show()
     */
    //创建自定义函数对象
   val myAvgNew = new MyAvgNew
    //将df转为ds
    val ds: Dataset[User02] = df.as[User02]
    //将自定义函数对象转换为查询列
    val col: TypedColumn[User02, Double] = myAvgNew.toColumn
    //在进行查询的时候，会将查询出来的记录(User02类型)交给自定义的函数进行处理
    ds.select(col).show()
    //
    //关闭连接
    spark.stop()
    
  }
}
//定义输入类型的样例类
case class User02(name:String,age:Long)
//缓存类型
case class AgeBuffer(var sum:Long,var count:Long)
//自定义UDAF函数(强类型)
/**
 * * @tparam IN  输入数据类型
 * * @tparam BUF 缓存数据类型
 * * @tparam OUT 输出结果类型
 */
class MyAvgNew extends Aggregator[User02,AgeBuffer,Double]{
  //对缓存数据进行初始化
  override def zero: AgeBuffer = {
    AgeBuffer(0L,0L)
  }
  //对当前分区数据进行聚合
  override def reduce(b: AgeBuffer, a: User02): AgeBuffer = {
    b.sum += a.age
    b.count += 1
    b
  }
  //分区间合并
  override def merge(b1: AgeBuffer, b2: AgeBuffer): AgeBuffer = {
    b1.sum += b2.sum
    b1.count +=b2.count
    b1
  }
  //返回计算结果
  override def finish(buffer: AgeBuffer): Double = {
    buffer.sum.toDouble/buffer.count
  }
  //DataSet编码解码器，用于进行序列化,固定写法
  //用户自定义Ref类型 product   系统值类型 根据具体类型进行选择
  override def bufferEncoder: Encoder[AgeBuffer] = {
    Encoders.product
  }

  override def outputEncoder: Encoder[Double] = {
    Encoders.scalaDouble
  }
}
