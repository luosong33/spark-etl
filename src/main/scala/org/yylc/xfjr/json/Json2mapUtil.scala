package org.yylc.xfjr.json

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SQLContext
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.spark._
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.mutable

object Json2mapUtil {

  //  传入字段，返回对应的值
  def json2map(columns: String, flag: String): mutable.Map[String, String] = {

    val startTime = System.currentTimeMillis()

    val conf = new SparkConf().setMaster("local[2]")
    val spark = SparkSession
      .builder()
      .appName("Json2map")
      .config(conf)
      .getOrCreate()
    val sc = spark.sparkContext

//    var path = "data/json/0c53547271974bcd93fde1bb6a8c477f.json"
    var path = "data/json/"
    val peopleDF = spark.read.json(path)
    if (flag == "key"){
      val peopleDF_ = peopleDF.select(columns)
      val value = peopleDF_.first.get(0).toString()
      val cluValue = scala.collection.mutable.Map(columns -> value)
      return cluValue
    }

    val frame = peopleDF.select(explode(peopleDF(columns)))
    val value = frame.first.get(0).toString() //  (phone_num_loc,香港)

    val strArr = columns.split("\\.")
    val column = strArr(strArr.length - 1)
    val cluValue = scala.collection.mutable.Map(column -> value)

    sc.stop()
    val endTime = System.currentTimeMillis()
    println("耗时为： " + (endTime - startTime))
    cluValue
  }

  def main(args: Array[String]): Unit = {
    val cluValue_ = json2map("cell_phone_number","key").get("cell_phone_number").toString
    val cluValue = json2map("cell_phone_number","key")
    var str = ""
    for (elem <- cluValue) {
      println(elem._2)
      str = elem._2
    }
  }


}
