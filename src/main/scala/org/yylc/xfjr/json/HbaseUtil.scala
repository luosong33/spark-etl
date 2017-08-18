package org.yylc.xfjr.json

import java.util

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.{SparkConf, SparkContext}
import org.yylc.xfjr.util.IdWorker

import scala.collection.JavaConversions._
import scala.collection.mutable

object HbaseUtil {

  //  para 一行，map：字段
  def ContactListHBase(tablename: String, rowKey: String, list: util.ArrayList[util.ArrayList[mutable.Map[String, String]]]): Unit = {
    val sparkConf = new SparkConf().setAppName("SparkWriteHBase").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)

    //    val tablename = "communication:contact_list"
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set("hbase.zookeeper.quorum", "node1")
    conf.set(TableOutputFormat.OUTPUT_TABLE, tablename)

    /*sc.hadoopConfiguration.set(TableOutputFormat.OUTPUT_TABLE, tablename)
    val job = new Job(sc.hadoopConfiguration)*/
    val job = Job.getInstance(conf)
    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setOutputValueClass(classOf[Result])
    job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])

    val arr = list.toArray()
    //    val indataRDD = sc.makeRDD(Array("3,Rongcheng,M,26", "4,Guanhua,M,27")) //构建两行记录
    val indataRDD = sc.makeRDD(list) //构建两行记录
    val stringToStrings = indataRDD.collect()
    //    val rdd = indataRDD.map(_.split(',')).map { arr => {
    val rdd = indataRDD.map(x => x.map { kv => {
      //    val rdd = indataRDD.map { kv => {
      val put = new Put(Bytes.toBytes(rowKey)) //行健的值
      //      kv.keys.foreach { i => println(i+":::::::::::"+kv(i)) }
      kv.keys.foreach { i => put.add(Bytes.toBytes("c"), Bytes.toBytes(i), Bytes.toBytes(kv(i))) }
      //      put.add(Bytes.toBytes("c"), Bytes.toBytes(arr(0)), Bytes.toBytes(arr(1))) //info:name列的值
      //      put.add(Bytes.toBytes("c"), Bytes.toBytes("gender"), Bytes.toBytes(arr(2))) //info:gender列的值
      //      put.add(Bytes.toBytes("c"), Bytes.toBytes("age"), Bytes.toBytes(arr(3).toInt)) //info:age列的值
      (new ImmutableBytesWritable, put)
    }
    })
    //    rdd.saveAsNewAPIHadoopDataset(job.getConfiguration())
    sc.stop()
  }

  //  para 一行，map：字段
  def sparkWriteHBase(tablename: String, rowKey: String, listMap: util.ArrayList[mutable.Map[String, String]]): Unit = {
    val sparkConf = new SparkConf().setAppName("SparkWriteHBase").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)

    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set("hbase.zookeeper.quorum", "node1")
    conf.set(TableOutputFormat.OUTPUT_TABLE, tablename)

    val job = Job.getInstance(conf)
    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setOutputValueClass(classOf[Result])
    job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])

    val arr = listMap.toArray()
    val indataRDD = sc.makeRDD(listMap) //构建两行记录
    val stringToStrings = indataRDD.collect()
    val rdd = indataRDD.map { kv => {
      val put = new Put(Bytes.toBytes(rowKey)) //行健的值
      kv.keys.foreach { i => put.add(Bytes.toBytes("c"), Bytes.toBytes(i), Bytes.toBytes(kv(i))) }
      (new ImmutableBytesWritable, put)
    }
    }
    rdd.saveAsNewAPIHadoopDataset(job.getConfiguration())
    sc.stop()
  }

  def main(args: Array[String]): Unit = {

    //  提交spark插入成功：spark2-submit --class org.yylc.xfjr.json.HbaseUtil --master yarn --deploy-mode client --driver-memory 4g
    //  --executor-memory 2g --executor-cores 1 --queue thequeue /bigdata/ls/spark-etl-1.0.0.jar
    //  使用  --deploy-mode client 只插入一条，cluster插入两条
    val rowKey = RowKeyUtil.rowKeyUtil("13313131313")
    var listMap = new util.ArrayList[mutable.Map[String, String]]()
    val cluValue = scala.collection.mutable.Map("name" -> "郭秀")
    listMap.add(cluValue)
    sparkWriteHBase("communication:contact_list", rowKey, listMap)
  }

}
