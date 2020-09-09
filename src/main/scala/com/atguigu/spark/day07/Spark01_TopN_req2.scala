package com.atguigu.spark.day07

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

/**
 * 统计热门品类的topN中，活跃用户的topN
 */
object Spark01_TopN_req2 {
  def main(args: Array[String]): Unit = {
    //2.创建Spark配置文件对象
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark01_TopN_req2")
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
//    res.foreach(println)
    //----------------------------------------------------需求二-------------------------------------------
    //1.获取热门品类top10的品类id
val ids: Array[String] = res.map(_.categoryId)
    //ids可以进行优化，因为发送给Excutor中的task使用，每一个task都会创建一个副本，所以可以使用广播变量
    val broadcastIds: Broadcast[Array[String]] = sc.broadcast(ids)
//    println(ids.mkString(","))
    /**
     * 将原始数据进行过滤（1.保留热门品类 2.只保留点击操作）
     * 对session的点击数进行转换 (category-session,1)
     * 对session的点击数进行统计 (category-session,sum)
     * 将统计聚合的结果进行转换  (category,(session,sum))
     * 将转换后的结构按照品类进行分组 (category,Iterator[(session,sum)])
     * 对分组后的数据降序 取前10
     */
    //2.将原始数据进行过滤（1.保留热门品类 2.只保留点击操作）
    val filterRdd: RDD[UserVisitAction] = actionRdd.filter {
      action => {
        //只保留点击行为
        if (action.click_category_id != -1) {
          //同时确定是热门品类的点击
          //坑 集合类型为字符串类型，id是long类型，需要进行转换
          broadcastIds.value.contains(action.click_category_id.toString)
        } else {
          false
        }
      }
    }
    
    //3.对session的点击数进行转换 (category-session,1)
    val mapRdd1: RDD[(String, Int)] = filterRdd.map {
      action => {
        (action.click_category_id + "_" + action.session_id, 1)
      }
    }
    //4.对session的点击数进行统计 (category-session,sum)
    val reduceRdd1: RDD[(String, Int)] = mapRdd1.reduceByKey(_ + _)
    //5.将统计聚合的结果进行转换  (category,(session,sum))
    val mapRdd2: RDD[(String, (String, Int))] = reduceRdd1.map {
      case (categorAndSession, sum) => {
        (categorAndSession.split("_")(0), (categorAndSession.split("_")(1), sum))
      }
    }
    //6.将转换后的结构按照品类进行分组 (category,Iterator[(session,sum)])
    val groupRdd2: RDD[(String, Iterable[(String, Int)])] = mapRdd2.groupByKey()
    //7.对分组后的数据降序 取前10
    val resRdd: RDD[(String, List[(String, Int)])] = groupRdd2.mapValues {
      datas => {
        datas.toList.sortWith {
          case (left, right) => {
            left._2 > right._2
          }
        }
      }.take(10)
    }
    resRdd.foreach(println)
    //关闭连接
    sc.stop()
  }
}
