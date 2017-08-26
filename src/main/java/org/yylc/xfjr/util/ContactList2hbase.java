package org.yylc.xfjr.util;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ContactList2hbase {

    private static int count = 0 ;
    private static List<Map<String, String>> countListMap = new ArrayList<>();

    //  入口调度方法，读文件夹、文件，（使用kafka消费）、调用解析、入库
    public static void file2hbase(String path) {
        File file = new File(path);
        if (file.exists()/* && file.isDirectory()*/) {
            File[] files = file.listFiles();
            if (/*files != null && */files.length == 0) {
                System.out.println("文件夹是空的!");
                return;
            } else {
                for (File file2 : files) {
                    if (file2.isDirectory()) {
                        System.out.println("文件夹:" + file2.getAbsolutePath());
                        file2hbase(file2.getAbsolutePath());
                    } else {
                        String file2path = file2.getAbsolutePath();
                        System.out.println("文件:" + file2.getAbsolutePath());
                        try {
                            FileInputStream inputStream = new FileInputStream(file2path);
                            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
                            String s = null;
                            int i = 0;
                            while ((s = bufferedReader.readLine()) != null) {
                                List<Map<String, String>> listMap = JsonUtil.json2listMap(s);  //  解析json
                                HBaseBasic02.putBach("communication:contact_list", "c", listMap);
                                i = listMap.size();
                                count += listMap.size();
                            }
//                            System.out.println("int=================="+i);
                            System.out.println("count=================="+count);
                            inputStream.close();
                            bufferedReader.close();

                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
        }/*else if (file.isFile()){  //  处理文件
            try {
                FileInputStream inputStream = new FileInputStream(path);
                BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
                String s = null;
                int i = 0;
                while ((s = bufferedReader.readLine()) != null) {
                    List<Map<String, String>> listMap = JsonUtil.json2listMap_w(s);  //  解析json
                    i = listMap.size();
                    count += listMap.size();
                }
//                System.out.println("int=================="+i);
                System.out.println("count=================="+count);
                inputStream.close();
                bufferedReader.close();

            } catch (IOException e) {
                e.printStackTrace();
            }
        }*/else {
            System.out.println("文件或文件夹不存在!");
        }
    }

    public static void main(String[] args) {
        long starttime = System.currentTimeMillis();
        file2hbase("/bigdata/ls/data_kafka");  //  linux
//        file2hbase("D:\\下载\\20170804\\kafkatest\\dataOrig.txt");  //  winds
        long endtime = System.currentTimeMillis();
        System.out.println("耗时为： " + (endtime - starttime));
    }

}
