package org.yylc.xfjr.json

import org.yylc.xfjr.util.IdWorker

object RowKeyUtil {

  /**
    * 1手机号  2时间戳
    * result  反转手机号_分布式唯一id
    */
  def rowKeyUtil(arg:String*): String = {
    val worker = new IdWorker(5)
    val nextId = worker.nextId
    val str = reverse(arg(0))
    val str1 = str+"_"+nextId
    str1
  }

  //  手机号反转，防止存储热点
  def reverse(args: String): String = {
    if (args.length == 0) args else reverse(args.substring(1, args.length)) + args.substring(0, 1)
  }

  def main(args: Array[String]): Unit = {
    val strDate = DateUtil.getStrTime("2017-02-12 00:00:35","yyyy-MM-dd H:mm:ss","yyyy-MM-dd-H-mm-ss")
    println(rowKeyUtil("18715818298",strDate))
  }

}
