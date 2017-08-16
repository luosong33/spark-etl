package org.yylc.xfjr.util;

import com.alibaba.fastjson.JSONReader;
import com.alibaba.fastjson.JSONWriter;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;


public class JsonUtil {

    //  反序列化大json数组
    public static void jsonBig(String[] args) {
        JSONReader reader = null;
        try {
            reader = new JSONReader(new FileReader("data/json/0d81957df4a8419984d9b9ef0fec2b32.json"));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        reader.startArray();
        while(reader.hasNext()) {
            Ontact_list ontact_list = reader.readObject(Ontact_list.class);
            // handle vo ...
            ArrayList<Map<String, String>> maps = new ArrayList<>();
            HashMap<String, String> columnMap = new HashMap<>();
            columnMap.put(ontact_list..,ontact_list.getCall_cnt())
        }
        reader.endArray();
        reader.close();
    }

    public static void main(String[] args) {

    }

}

