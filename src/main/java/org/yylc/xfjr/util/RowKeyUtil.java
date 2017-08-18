package org.yylc.xfjr.util;


public class RowKeyUtil {

    /**
     * 1手机号  2时间戳
     * result  反转手机号_分布式唯一id
     */
    public static String rowKeyUtil(String arg){
        IdWorker worker = new IdWorker(5);
        Long nextId = worker.nextId();
        String str = new StringBuilder(arg).reverse().toString();
        String str1 = str+"_"+nextId;
        return str1;
    }

    //  手机号反转，防止存储热点
//  def reverse(args: String): String = if (args.length == 0) args else reverse(args.substring(1, args.length)) + args.substring(0, 1)

  public static void main(String[] args){
//    DateUtil.getStrTime("2017-02-12 00:00:35","yyyy-MM-dd H:mm:ss","yyyy-MM-dd-H-mm-ss")
//    println(rowKeyUtil("18715818298",strDate))
  }

}
