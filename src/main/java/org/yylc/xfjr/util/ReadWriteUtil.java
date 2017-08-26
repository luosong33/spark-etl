package org.yylc.xfjr.util;

import java.io.*;
import java.util.List;
import java.util.Map;

public class ReadWriteUtil {
    private static String pathWrite = "";

    //  循环读取文件夹下所有文件
    public static void reads(String path,String pathWrite) {
        File file = new File(path);
        if (file.exists()) {
            File[] files = file.listFiles();
            if (files.length == 0) {
                System.out.println("文件夹是空的!");
                return;
            } else {
                for (File file2 : files) {
                    if (file2.isDirectory()) {
                        System.out.println("文件夹:" + file2.getAbsolutePath());
                        reads(file2.getAbsolutePath(),pathWrite);
                    } else {
                        String file2path = file2.getAbsolutePath();
                        String[] split = file2path.split("/");  //  linux
//                        String[] split = file2path.split("\\\\");
                        int length = split.length;
                        String name = split[length - 1];
                        String filePath = pathWrite + "/" + name;  //  linux
//                        String filePath = pathWrite + "\\" + name;  //  windows
                        System.out.println("文件:" + file2.getAbsolutePath());
                        try {
                            FileInputStream inputStream = new FileInputStream(file2path);
                            //  字节流中指定编码
                            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream,"GB2312"));
                            BufferedWriter out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File(filePath/*+"_utf"*/)), "UTF-8"));
                            String s = null;
                            StringBuffer sb = new StringBuffer();
                            while ((s = bufferedReader.readLine()) != null) {
                                //  逻辑
                                sb.append(s+"\r\n");
                            }
                            out.write(sb.toString());
                            out.flush();
                            out.close();
                            inputStream.close();
                            bufferedReader.close();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
        } else {
            System.out.println("文件不存在!");
        }
    }

    //  写单个文件
    public static void write(String path,String content){
        try {
            /* 写入文件 */
            File file = new File(path);
            if (!file.exists()) {  // 如果文件不存在则创建
                file.createNewFile();
            }
//            BufferedWriter out = new BufferedWriter(new FileWriter(writename));
            BufferedWriter out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file, true), "UTF-8"));  //  追加  加true
            out.write(content+"\r\n"); // \r\n即为换行
            out.flush(); // 把缓存区内容压入文件
            out.close(); // 最后记得关闭文件
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //  1、输入读取目录  2、输出目录
    public static void main(String[] args) {
        long starttime = System.currentTimeMillis();
        reads(args[0],args[1]);  //  linux
//        pathWrite = args[1];  //  调用不过去，可能是值传递的原因
//        reads("/bigdata/ls/data_kafka");  //  linux
//        reads("D:\\下载\\20170804\\kafkatest");  //  winds
        long endtime = System.currentTimeMillis();
        System.out.println("耗时为： " + (endtime - starttime));
    }

}
