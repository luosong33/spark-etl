package org.yylc.xfjr.json

import java.util

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.JavaConversions._
import scala.collection.mutable

object HbaseUtil {

  //  para 一行，map：字段
  def sparkWriteHBase(rowKey: String, listMap: util.ArrayList[mutable.Map[String, String]]): Unit = {
    val sparkConf = new SparkConf().setAppName("SparkWriteHBase").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)

    val tablename = "communication:contact_list"
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set("hbase.zookeeper.quorum", "no1")
    conf.set(TableOutputFormat.OUTPUT_TABLE, tablename)

    /*sc.hadoopConfiguration.set(TableOutputFormat.OUTPUT_TABLE, tablename)
    val job = new Job(sc.hadoopConfiguration)*/
    val job = Job.getInstance(conf)
    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setOutputValueClass(classOf[Result])
    job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])

    val arr = listMap.toArray()
    //    val indataRDD = sc.makeRDD(Array("3,Rongcheng,M,26", "4,Guanhua,M,27")) //构建两行记录
    val indataRDD = sc.makeRDD(listMap) //构建两行记录
    val stringToStrings = indataRDD.collect()
    //    val rdd = indataRDD.map(_.split(',')).map { arr => {
    val rdd = indataRDD.map { kv => {
      val put = new Put(Bytes.toBytes(rowKey)) //行健的值
      kv.keys.foreach { i => println(i+":::::::::::"+kv(i)) }
      kv.keys.foreach { i => put.add(Bytes.toBytes("c"), Bytes.toBytes(i), Bytes.toBytes(kv(i))) }
//      put.add(Bytes.toBytes("c"), Bytes.toBytes(arr(0)), Bytes.toBytes(arr(1))) //info:name列的值
//      put.add(Bytes.toBytes("info"), Bytes.toBytes("gender"), Bytes.toBytes(arr(2))) //info:gender列的值
//      put.add(Bytes.toBytes("info"), Bytes.toBytes("age"), Bytes.toBytes(arr(3).toInt)) //info:age列的值
      (new ImmutableBytesWritable, put)
    }
    }
    rdd.saveAsNewAPIHadoopDataset(job.getConfiguration())
    sc.stop()
  }

}
