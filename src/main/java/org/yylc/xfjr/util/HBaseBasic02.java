package org.yylc.xfjr.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class HBaseBasic02 {
    private static Configuration conf;
    private static Connection conn;
    // 初始化连接
    static {
        conf = HBaseConfiguration.create(); // 获得配制文件对象
        conf.set("hbase.zookeeper.quorum", "rscala");
        try {
            conn = ConnectionFactory.createConnection(conf);// 获得连接对象
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    // 获得连接
    public static Connection getConn() {
        if (conn == null || conn.isClosed()) {
            try {
                conn = ConnectionFactory.createConnection(conf);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return conn;
    }

    // 单条插入
    public static void put1(String tableName, String rowKey, String family, String column, String value) {
//        Configuration conf = init();
//        Connection conn = getConn();
        try {
//            HBaseAdmin admin = new HBaseAdmin(conf);
            Admin admin = getConn().getAdmin();
//            HTable table = new HTable(conf, TableName.valueOf(tableName));
            Table table = getConn().getTable(TableName.valueOf(tableName));
            //判断表是否存在，如果不存在进行创建
//            if (!admin.tableExists(Bytes.toBytes(tableName))) {
            if (!admin.tableExists(TableName.valueOf(Bytes.toBytes(tableName)))) {
//                HTableDescriptor tableDescriptor = new HTableDescriptor(Bytes.toBytes(tableName));
                HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(tableName));
                HColumnDescriptor columnDescriptor = new HColumnDescriptor(Bytes.toBytes(family));
                admin.createTable(htd);
            }
//            table.setAutoFlush(true);
            //进行数据插入
            Put put = new Put(Bytes.toBytes(rowKey));
            put.add(Bytes.toBytes(String.valueOf(family)), Bytes.toBytes(column), Bytes.toBytes(value));
            table.put(put);
            table.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
    // 单条插入
    public static void put_bach(String tableName, String family, List<Map<String, String>> listMap) {
//        Configuration conf = init();
//        Connection conn = getConn();
        try {
            Admin admin = getConn().getAdmin();
            Table table = getConn().getTable(TableName.valueOf(tableName));
            //判断表是否存在，如果不存在进行创建
            if (!admin.tableExists(TableName.valueOf(Bytes.toBytes(tableName)))) {
                HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(tableName));
                HColumnDescriptor columnDescriptor = new HColumnDescriptor(Bytes.toBytes(family));
                admin.createTable(htd);
            }
//            table.setAutoFlush(true);
            //进行数据插入
            List<Put> putList=new ArrayList<>();
            for (Map<String, String> map : listMap){
                String phone_num = map.get("phone_num");
                String rowKey = RowKeyUtil.rowKeyUtil(phone_num);
                Put put = new Put(Bytes.toBytes(rowKey));
                for (Map.Entry<String, String> entry : map.entrySet()) {
                    System.out.println("Key = " + entry.getKey() + ", Value = " + entry.getValue());
                    put.add(Bytes.toBytes(String.valueOf(family)), Bytes.toBytes(entry.getKey()), Bytes.toBytes(entry.getValue()));
                }
                putList.add(put);
            }
            table.put(putList);
            table.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    private Configuration init() {

        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum","192.168.10.149,192.168.10.44,192.168.10.49");
        config.set("hbase.zookeeper.property.clientPort", "2181");
        /*conf.set("fs.defaultFS", "hdfs://master129:9000/");
        conf.set("mapreduce.framework.name", "local");
        conf.set("mapred.job.tracker", "master129:9001");
        conf.set("hbase.zookeeper.quorum", "master129,slave130,slave131,slave132");*/
        return config;
    }

}
