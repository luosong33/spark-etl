package org.yylc.xfjr.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.JSONReader;

import javax.swing.text.BadLocationException;
import javax.swing.text.DefaultStyledDocument;
import javax.swing.text.rtf.RTFEditorKit;
import java.io.*;
import java.util.*;


public class JsonUtil {

    //  处理json字符串
    public static List<Map<String, String>> json2listMap(String json) {
        ArrayList<Map<String, String>> listMap = new ArrayList<>();
        ReadWriteUtil.write("/bigdata/ls/kafka_error/dataOrig.txt",json);

        JSONObject jobj = null;
        try {
            jobj = JSON.parseObject(json);
            JSONObject push_data = (JSONObject) jobj.get("push_data");
            JSONArray arr = push_data.getJSONArray("contact_list");  //  数组
            for (Iterator iterator = arr.iterator(); iterator.hasNext(); ) {
                HashMap<String, String> hash = new HashMap<>();
                JSONObject job = (JSONObject) iterator.next();  //  每条数据
                Set<String> keys = job.keySet();
                for (String key : keys) {  //  一条所有字段
//                System.out.println(key+"::::::::::::::"+job.get(key));
                    hash.put(key, job.get(key).toString());
                }
                listMap.add(hash);
            }
        } catch (Exception e) {
//            e.printStackTrace();
            System.out.println("===================json解析错误:==================="/*+json*/);
            ReadWriteUtil.write("/bigdata/ls/kafka_error/dataError.txt",json);
        }
        return listMap;
    }

    //  处理json字符串
    public static List<Map<String, String>> json2listMap_w(String json) {
        ArrayList<Map<String, String>> listMap = new ArrayList<>();
        JSONObject jobj = null;
        try {
            jobj = JSON.parseObject(json);
            JSONObject push_data = (JSONObject) jobj.get("push_data");
            JSONArray arr = push_data.getJSONArray("contact_list");  //  数组
            for (Iterator iterator = arr.iterator(); iterator.hasNext(); ) {
                HashMap<String, String> hash = new HashMap<>();
                JSONObject job = (JSONObject) iterator.next();  //  每条数据
                Set<String> keys = job.keySet();
                for (String key : keys) {  //  一条所有字段
                    hash.put(key, job.get(key).toString());
                }
                listMap.add(hash);
            }
        } catch (Exception e) {
            System.out.println("===================json解析错误:==================="/*+json*/);
        }
        return listMap;
    }


    //  反序列化大json数组
    public static void jsonBig(String[] args) {
        JSONReader reader = null;
        try {
            reader = new JSONReader(new FileReader("data/json/0d81957df4a8419984d9b9ef0fec2b32.json"));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        reader.startArray();
        while (reader.hasNext()) {
            OntactistLEntity ontact_list = reader.readObject(OntactistLEntity.class);
            // handle vo ...
            ArrayList<Map<String, String>> maps = new ArrayList<>();
            HashMap<String, String> columnMap = new HashMap<>();
//            columnMap.put(ontact_list..,ontact_list.getCall_cnt())
        }
        reader.endArray();
        reader.close();
    }

    public static void main(String[] args) {

        List<Map<String, String>> listMap = json2listMap("{\"name\":\"郭秀\",\"push_data\":{\"contact_list\":[{\"contact_noon\":0,\"phone_num_loc\":\"香港\",\"contact_3m\":1,\"contact_1m\":0,\"contact_1w\":0,\"p_relation\":\"\",\"phone_num\":\"0085264531663\",\"contact_name\":\"未知\",\"call_in_cnt\":1,\"call_out_cnt\":0,\"call_out_len\":0.0,\"contact_holiday\":0,\"needs_type\":\"未知\",\"contact_weekday\":1,\"contact_afternoon\":0,\"call_len\":0.1,\"contact_early_morning\":0,\"contact_night\":1,\"contact_3m_plus\":0,\"call_cnt\":1,\"call_in_len\":0.1,\"contact_all_day\":false,\"contact_morning\":0,\"contact_weekend\":0}]}}");

//        System.out.println();
    }

    public static String readRtf(File in) {
        RTFEditorKit rtf = new RTFEditorKit();
        DefaultStyledDocument dsd = new DefaultStyledDocument();
        String text = "";
        try {
            rtf.read(new FileInputStream(in), dsd, 0);
            text = new String(dsd.getText(0, dsd.getLength()));
        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (BadLocationException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return text;
    }

}

