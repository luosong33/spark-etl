package org.yylc.xfjr.util;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.yylc.xfjr.util.OperateFilesUtils.traverseFolder;

public class ContactList2hbase {

    //  入口调度方法，读文件夹、文件，（使用kafka消费）、调用解析、入库
    public static void contactList2hbase(String path){
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
                        traverseFolder(file2.getAbsolutePath());
                    } else {
                        String file2path = file2.getAbsolutePath();
                        System.out.println("文件:" + file2.getAbsolutePath());
                        try {
                            String s = OperateFilesUtils.readText1(file2path);  //  读取文件;或者消费kafka

                            List<Map<String, String>> listMap = JsonUtil.json2listMap(s);  //  解析json
                            /*for (Map<String, String> map : listMap){
                                String phone_num = map.get("phone_num");
                                HbaseUtil.sparkWriteHBase("communication:contact_list",phone_num,  listMap_);
                            }*/
                            HBaseBasic02.put_bach("communication:contact_list","c",listMap);

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


    public static void main(String[] args) {
//        contactList2hbase("/bigdata/ls/data/0c53547271974bcd93fde1bb6a8c477f.json");  //  linux
        contactList2hbase("E:\\git\\spark-etl\\data\\json");  //  winds
    }

}
