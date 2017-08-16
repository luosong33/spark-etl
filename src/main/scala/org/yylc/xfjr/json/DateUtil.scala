package org.yylc.xfjr.json

import java.text.SimpleDateFormat
import java.util.Date



object DateUtil {

  // 将字符串转为时间戳,首先将字符串按固定格式转成时间格式，然后就可以转成其它格式的字符串
  def getStrTime(user_timestr: String,format1: String,format2: String): String = {

    //  按对应格式转date
    val format2date = new SimpleDateFormat(format1)
    val date = format2date.parse(user_timestr)
    //  按指定格式转str
    val format2str = new SimpleDateFormat(format2)
    val str = format2str.format(date)

    str
  }


  def main(args: Array[String]): Unit = {
    val time = "2017-02-12 00:00:35"
    val re_str = getStrTime(time,"yyyy-MM-dd H:mm:ss","yyyy-MM-dd-H-mm-ss")
    println(re_str)
  }

}
