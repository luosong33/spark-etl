package org.yylc.xfjr.json

import java.util

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.yylc.xfjr.json.Json2mapUtil.json2map
import org.yylc.xfjr.util.IdWorker

import scala.collection.JavaConversions._
import scala.collection.mutable
//import sqlContext.implicits._

object Json2Hbase {

  def json2hbase(args: String): Unit = {

    val startTime = System.currentTimeMillis()
    //  spark上下文入口
    /*val conf = new SparkConf().setAppName("Json2Hbase").setMaster("local[*]")
//    var dataPath = args(0)
    var dataPath = "data/json/"
    val sc = new SparkContext(conf)*/

    val conf = new SparkConf()
    //    if (args.length == 0) {
    conf.setMaster("local[2]")
    //    }
    val spark = SparkSession
      .builder()
      .appName("Json2Hbase_")
      .config(conf)
      .getOrCreate()
    val sc = spark.sparkContext
    //    val sqlContext = new SQLContext(sc)

    //  读取本地json文件，每一行需为json对象 ｛｝
    var path = "data/json/0c53547271974bcd93fde1bb6a8c477f.json"
    val peopleDF = spark.read.json(path)
    /*val _peopleDF = sqlContext.jsonFile(path)  //  等同first、和collect一样；[mobile,18715818298,510525198609204363,郭秀,[5983dece603e8526381fa7d0,WrappedArray([是否绑......
    _peopleDF.collect().foreach(x => println(x))*/
    //    peopleDF.printSchema()  //  调试打印树形格式
    //    peopleDF.show(5)  //  |  mobile|      18715818298|  510525198609204363|  郭秀|[5983dece603e8526...|report|   |chinamobilesc|
    //    peopleDF.select("name").show()  //  没加show就是类型
    //    peopleDF.collect().foreach(x => println(x))  //  只有值；同first；[mobile,18715818298,510525198609204363,郭秀,[5983dece603e8526381fa7d0,WrappedArray([是否绑定运......
    //    println(peopleDF.select("push_data").first()) // 全循环，没有name，只有值；和collect一样
    println("-------------------------原始↑---------------------------")
    //    val peopleDF_ = peopleDF.select("push_data").select("contact_list") // .select(explode(peopleDF("contact_list")))
    //    val peopleDF_ = peopleDF.select("push_data.contact_list") // explode(peopleDF("contact_list")   //  |[[1,1,0.1,0.1,0,0...|
    val peopleDF_ = peopleDF.select("cell_phone_number")
    val value_ = peopleDF_.first.get(0).toString() //  18715818298
    println(value_)
    //    peopleDF_.printSchema()
    //    peopleDF_.collect().foreach(x => println(x))  //  [WrappedArray([1,1,0.1,0.1,0,0.0,0,0,1,0,0,false,0,0,0,未知,1,0,1,0,未知,,0085264531663,香港])]
    peopleDF_.show(3) //  |[[1,1,0.1,0.1,0,0...|
    println("------------------------------------------------------")
    //    peopleDF_.toDF().show()  //  |[[1,1,0.1,0.1,0,0...|
    val frame = peopleDF.select(explode(peopleDF("push_data.contact_list.phone_num_loc")))
    val value = frame.first.get(0).toString() //  |phone_num_loc|       |         [香港]|
    //    val rows = frame.first.get(0) //  phone_num_loc  [香港]
    //    println("columns:::::::::::::::"+columns(0))
    //    val rows = frame.collect()
    //    rows.toString.length
    //    println("value:::::::::::::::"+rows(13,rows.length-1))
    //    rows.map(x => println(x.get(0)))
    //    val row = rows(0).get(0).toString
    //    println(rows)
    //    val value = rows.split("(")(0)


    //    val peopleDF_list = peopleDF_.select(explode(peopleDF_("contact_list")))
    //    peopleDF_list.show()
    //    println(peopleDF_list.columns)
    //    val rows = peopleDF_.collect()
    //    peopleDF_.foreach(x => println(x))

    //    cluValue.clear()
    val cluValue = scala.collection.mutable.Map("phone_num_loc" -> value)
    for (elem <- cluValue) {
      println(elem)
    }

    //  示例2
    /*val df = spark.read.json("data/json/sample.txt")
    df.show
    println("-------------------------原始↑---------------------------")
    val dfDates = df.select(explode(df("dates")))
    dfDates.show()*/

    val endTime = System.currentTimeMillis()
    println("耗时为： " + (endTime - startTime))

  }


  /* 订单详情 */
  def json2hbaseContact_list(): Unit = {
    val strPrefix = "push_data.contact_list" //  前缀
    //  对应值的key
    val strArr: Array[String] = Array("call_cnt", "call_in_cnt", "call_in_len", "call_len", "call_out_cnt", "call_out_len", "contact_1m",
      "contact_1w", "contact_3m", "contact_3m_plus", "contact_afternoon", "contact_all_day", "contact_early_morning", "contact_holiday",
      "contact_morning", "contact_name", "contact_night", "contact_noon", "contact_weekday", "contact_weekend", "needs_type", "phone_num", "phone_num_loc", "p_relation")
    val listColum = new util.ArrayList[String]()
    val listValue = new util.ArrayList[String]()
    var listMap = new util.ArrayList[mutable.Map[String, String]]()
    for (x <- strArr) { //  循环所有字段
      val cluValue = json2map(strPrefix + "." + x, "") //  取值
      listMap.add(cluValue)
      /*for (elem <- cluValue) {
        listColum.add(elem._1)
        listValue.add(elem._2)
      }*/
    } //  调用hbase入库方法
    println("---------------↓----------------")
    //    val arrColum = listColum.toArray()
    //    for (i <- arrColum) println("arrColum Element: " + i)
    val arr = listMap.toArray()
    for (i <- listMap) {
      for (elem <- i) {
        println(elem)
      }
    }
    println("---------------↑----------------")

    val phone = json2map("cell_phone_number", "key")
    var str = ""
    for (elem <- phone) { str = elem._2 }
    val rowKey = RowKeyUtil.rowKeyUtil(str)
    HbaseUtil.sparkWriteHBase("communication:contact_list",rowKey, listMap)
  }


  def main(args: Array[String]): Unit = {
    json2hbaseContact_list()
  }


}
