package com.atguigu.spark.day07

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

/**
 * 计算页面单跳转换率
 */
object Spark01_TopN_req3 {
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
    //====================================需求三实现=======================================
    //1.对当前日志中记录的访问页面进行计数
    val pageIdRdd: RDD[(Long, Long)] = actionRdd.map {
      action => {
        (action.page_id, 1L)
      }
    }
    //2.通过页面的计数，计算每一个页面出现的总次数 作为求单跳转换率的分母
    val fmIdsMap: Map[Long, Long] = pageIdRdd.reduceByKey(_ + _).collect().toMap
    //3.计算分子
    //3.1将原始数据根据sessionId进行分组
    val sessionRdd: RDD[(String, Iterable[UserVisitAction])] = actionRdd.groupBy(_.session_id)
    //3.2将分组后的数据按时间进行升序排序
    val pageFlowRdd: RDD[(String, List[(String, Int)])] = sessionRdd.mapValues {
      datas => {
        //3.3得到排序后的同一个session的用户访问行为
        val userActions: List[UserVisitAction] = datas.toList.sortWith {
          (left, right) => {
            left.action_time < right.action_time
          }
        }
        //3.4对排序后的用户访问行为进行结构转换，只保留页面就可以
        val pageIdLists: List[Long] = userActions.map(_.page_id)
        //A-B-C-D-E-F
        //B-C-D-E-F
        //3.5对当前会话用户访问页面进行拉链,得到页面流转情况(页面A,页面B)
        val pageFlows: List[(Long, Long)] = pageIdLists.zip(pageIdLists.tail)
        //3.6对拉链后的数据进行结构转换(页面A-页面B,1)
        pageFlows.map {
          case (pageId1, pageId2) => {
            (pageId1 + "-" + pageId2, 1)
          }
        }
      }
    }
    //3.7将每一个会话的页面跳转统计完毕之后，没有必要保留会话信息了，所以对上述RDD的结构进行转换
    // 只保留页面跳转以及计数
    val pageFlowMapRdd: RDD[(String, Int)] = pageFlowRdd.map(_._2).flatMap(list => list)
    //3.7对页面跳转情况进行聚合操作
    val pageAToPageBSumRdd: RDD[(String, Int)] = pageFlowMapRdd.reduceByKey(_ + _)
    //4.页面单跳转换率计算
    pageAToPageBSumRdd.foreach{
      //(pageA-pageB,sum)
      case (pageFlow,fz) =>{
        val pageIds: Array[String] = pageFlow.split("-")
        //获取分母页面id
        val fmPageId: Long = pageIds(0).toLong
        //根据分母页面id,获取分母页面总访问数
        val fmSum: Long = fmIdsMap.getOrElse(fmPageId, 1L)
        //转换率
        println(pageFlow+"------>"+fz.toDouble / fmSum)
      }
    }
    
    
    //关闭连接
    sc.stop()
  }
}
