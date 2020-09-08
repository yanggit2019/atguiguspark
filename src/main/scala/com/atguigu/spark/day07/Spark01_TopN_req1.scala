package com.atguigu.spark.day07

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

/**
 * 统计热门品类TopN
 */
object Spark01_TopN_req1 {
  def main(args: Array[String]): Unit = {
    //2.创建Spark配置文件对象
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark01_CreateRDD_mem")
    //1.创建SparkContext对象
    val sc = new SparkContext(conf)
    //1.读取数据，创建rdd
    val dataRdd: RDD[String] = sc.textFile("H:\\spark-0105\\user_visit\\user_visit_action.txt")
    //2.将读到的数据进行切分，并且将切分的内容封装为UserVisitAction对象
    val actionRdd: RDD[UserVisitAction] = dataRdd.map {
      line => {
        val fields: Array[String] = line.split("_")
        UserVisitAction(
          fields(0),
          fields(1).toLong,
          fields(2),
          fields(3).toLong,
          fields(4),
          fields(5),
          fields(6).toLong,
          fields(7).toLong,
          fields(8),
          fields(9),
          fields(10),
          fields(11),
          fields(12).toLong
        )
      }
    }
    //3.判断当前这条日志记录的是什么行为，并且封装为结果对象(品类,点击数,下单数,支付数)===>例如:如果是鞋的点击行为(鞋，1，0，0)
    val infoRdd: RDD[CategoryCountInfo] = actionRdd.flatMap {
      userAction => {
        //判断是否为点击行为
        if (userAction.click_category_id != -1) {
          //封装输出结果对象
          List(CategoryCountInfo(userAction.click_category_id.toString, 1, 0, 0))

        } else if (userAction.order_category_ids != "null") { //坑：读取的文件应该是null字符串，而不是null对象
          //判断是否为下单行为,如果是下单行为，需要对当前订单中设计的所有品类id进行切分
          val ids: Array[String] = userAction.order_category_ids.split(",")
          //定义一个集合，用于存放多个品类id封装的输出结果对象
          val categoryCountInfoList: ListBuffer[CategoryCountInfo] = ListBuffer[CategoryCountInfo]()

          //对所有品类的id进行遍历
          for (id <- ids) {
            categoryCountInfoList.append(CategoryCountInfo(id, 0, 1, 0))
          }
          categoryCountInfoList
        } else if (userAction.pay_category_ids != "null") {
          //支付
          //判断是否为下单行为,如果是下单行为，需要对当前订单中设计的所有品类id进行切分
          val ids: Array[String] = userAction.pay_category_ids.split(",")
          //定义一个集合，用于存放多个品类id封装的输出结果对象
          val categoryCountInfoList: ListBuffer[CategoryCountInfo] = ListBuffer[CategoryCountInfo]()

          //对所有品类的id进行遍历
          for (id <- ids) {
            categoryCountInfoList.append(CategoryCountInfo(id, 0, 0, 1))
          }
          categoryCountInfoList
        } else {
          Nil
        }
      }
    }
    //将相同品类的放到一组
    val groupRdd: RDD[(String, Iterable[CategoryCountInfo])] = infoRdd.groupBy(_.categoryId)
    //将分组之后的数据进行聚合处理 (鞋,60,100,20,80)
    val reduceRdd: RDD[(String, CategoryCountInfo)] = groupRdd.mapValues {
      datas => {
        datas.reduce {
          (info1, info2) => {
            info1.clickCount = info1.clickCount + info2.clickCount
            info1.orderCount = info1.orderCount + info2.orderCount
            info1.payCount = info1.payCount + info2.payCount
            info1
          }
        }
      }
    }
    //对上述rdd进行转换，只保留value部分 得到聚合之后的RDD CategoryCountInfo
    val mapRdd: RDD[CategoryCountInfo] = reduceRdd.map(_._2)
    //7.对Rdd中的数据进行排序
    val res: Array[CategoryCountInfo] = mapRdd.sortBy(info => (info.clickCount, info.orderCount, info.payCount), false).take(10)
    //8.输出结果
    res.foreach(println)
    
    //关闭连接
    sc.stop()
  }
}
//用户访问动作表
case class UserVisitAction(date: String,//用户点击行为的日期
                           user_id: Long,//用户的ID
                           session_id: String,//Session的ID
                           page_id: Long,//某个页面的ID
                           action_time: String,//动作的时间点
                           search_keyword: String,//用户搜索的关键词
                           click_category_id: Long,//某一个商品品类的ID
                           click_product_id: Long,//某一个商品的ID
                           order_category_ids: String,//一次订单中所有品类的ID集合
                           order_product_ids: String,//一次订单中所有商品的ID集合
                           pay_category_ids: String,//一次支付中所有品类的ID集合
                           pay_product_ids: String,//一次支付中所有商品的ID集合
                           city_id: Long)//城市 id
// 输出结果表
case class CategoryCountInfo(categoryId: String,//品类id
                             var clickCount: Long,//点击次数
                             var orderCount: Long,//订单次数
                             var payCount: Long)//支付次数