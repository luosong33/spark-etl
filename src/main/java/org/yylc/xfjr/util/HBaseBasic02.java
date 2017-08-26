package org.yylc.xfjr.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HBaseBasic02 {
    private static Configuration conf;
    private static Connection conn;

    // 初始化连接
    static {
        conf = HBaseConfiguration.create(); // 获得配制文件对象
        conf.set("hbase.zookeeper.quorum", "3.3.3.126,3.3.3.127,3.3.3.128,3.3.3.129,3.3.3.130");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        try {
            conn = ConnectionFactory.createConnection(conf);// 获得连接对象
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // 获得连接
    public static Connection getConn() {
        Configuration conf = HBaseConfiguration.create(); // 获得配制文件对象
        conf.set("hbase.zookeeper.quorum", "3.3.3.126,3.3.3.127,3.3.3.128,3.3.3.129,3.3.3.130");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        Connection conn = null;
        if (conn == null || conn.isClosed()) {
            try {
                ConnectionFactory.createConnection(conf);// 获得连接对象
                conn = ConnectionFactory.createConnection(conf);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return conn;
    }

    // 关闭连接
    public static void close() {
        if (conn != null) {
            try {
                conn.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    // 单条插入
    public static void insert_put(String tableName, String rowKey, String family, String column, String value) {
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
                htd.addFamily(columnDescriptor);
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
    public static void put(String tableName, String family, Map<String, String> map) {
        try {
            Admin admin = getConn().getAdmin();
            Table table = getConn().getTable(TableName.valueOf(tableName));
            //判断表是否存在，如果不存在进行创建
            if (!admin.tableExists(TableName.valueOf(Bytes.toBytes(tableName)))) {
//                HTableDescriptor tableDescriptor = new HTableDescriptor(Bytes.toBytes(tableName));
                HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(tableName));
                HColumnDescriptor columnDescriptor = new HColumnDescriptor(Bytes.toBytes(family));
                htd.addFamily(columnDescriptor);
                admin.createTable(htd);
            }
            //进行数据插入
            String phone_num = map.get("phone_num");
            String rowKey = RowKeyUtil.rowKeyUtil(phone_num);
            Put put = new Put(Bytes.toBytes(rowKey));
            for (Map.Entry<String, String> entry : map.entrySet()) {
                put.add(Bytes.toBytes(String.valueOf(family)), Bytes.toBytes(entry.getKey()), Bytes.toBytes(entry.getValue()));
            }
            table.put(put);
            table.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    // 批量插入
    public static void putBach(String tableName, String family, List<Map<String, String>> listMap) {
        try {
            Admin admin = getConn().getAdmin();
            Table table = getConn().getTable(TableName.valueOf(tableName));
            //判断表是否存在，如果不存在进行创建
            if (!admin.tableExists(TableName.valueOf(Bytes.toBytes(tableName)))) {
                HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(tableName));
                HColumnDescriptor columnDescriptor = new HColumnDescriptor(Bytes.toBytes(family));
                admin.createTable(htd);
            }
            //进行数据插入
            List<Put> putList = new ArrayList<>();
            for (Map<String, String> map : listMap) {
                String phone_num = map.get("phone_num");
                String rowKey = RowKeyUtil.rowKeyUtil(phone_num);
                Put put = new Put(Bytes.toBytes(rowKey));
                for (Map.Entry<String, String> entry : map.entrySet()) {
//                    System.out.println("Key = " + entry.getKey() + ", Value = " + entry.getValue());
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

    // 批量插入
    public static void putBuff(String tableName, String family, List<Map<String, String>> listMap) {
        try {
            Admin admin = getConn().getAdmin();
            //判断表是否存在，如果不存在进行创建
            if (!admin.tableExists(TableName.valueOf(Bytes.toBytes(tableName)))) {
                HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(tableName));
                HColumnDescriptor columnDescriptor = new HColumnDescriptor(Bytes.toBytes(family));
                admin.createTable(htd);
            }

//            Table table = getConn().getTable(TableName.valueOf(tableName));
            HTable htable = new HTable(conf, TableName.valueOf(tableName));
            htable.setAutoFlush(false);
//            htable.setWriteBufferSize(64*1024*1024);  //  byte字节数
            htable.setWriteBufferSize(1024 * 1024);
            //进行数据插入
            List<Put> putList = new ArrayList<>();
            for (Map<String, String> map : listMap) {
                String phone_num = map.get("phone_num");
                String rowKey = RowKeyUtil.rowKeyUtil(phone_num);
                Put put = new Put(Bytes.toBytes(rowKey));
                for (Map.Entry<String, String> entry : map.entrySet()) {
                    put.add(Bytes.toBytes(String.valueOf(family)), Bytes.toBytes(entry.getKey()), Bytes.toBytes(entry.getValue()));
                }
                putList.add(put);
            }
            int length = listMap.size();
            /*for (Map<String, String> map : listMap){
                String phone_num = map.get("phone_num");
                String rowKey = RowKeyUtil.rowKeyUtil(phone_num);
                Put put = new Put(Bytes.toBytes(rowKey));
                for (Map.Entry<String, String> entry : map.entrySet()) {
                    put.add(Bytes.toBytes(String.valueOf(family)), Bytes.toBytes(entry.getKey()), Bytes.toBytes(entry.getValue()));
                }
                htable.put(put);
            }*/
            htable.put(putList);
            htable.flushCommits();
            htable.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    private Configuration init() {

        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", "192.168.10.149,192.168.10.44,192.168.10.49");
        config.set("hbase.zookeeper.property.clientPort", "2181");
        /*conf.set("fs.defaultFS", "hdfs://master129:9000/");
        conf.set("mapreduce.framework.name", "local");
        conf.set("mapred.job.tracker", "master129:9001");
        conf.set("hbase.zookeeper.quorum", "master129,slave130,slave131,slave132");*/
        return config;
    }


    // 创建表
    public static void createTable(String tableName, String... FamilyColumn) {
        TableName tn = TableName.valueOf(tableName);
        try {
            Admin admin = getConn().getAdmin();
            HTableDescriptor htd = new HTableDescriptor(tn);
            for (String fc : FamilyColumn) {
                HColumnDescriptor hcd = new HColumnDescriptor(fc);
                htd.addFamily(hcd);
            }
            admin.createTable(htd);
            admin.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // 删除表
    public static void dropTable(String tableName) {
        TableName tn = TableName.valueOf(tableName);
        try {
            Admin admin = conn.getAdmin();
            //先调用disable才可以删除
            admin.disableTable(tn);
            admin.deleteTable(tn);
            admin.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // 插入或者更新数据
    public static boolean insert(String tableName, String rowKey, String family, String qualifier, String value) {
        try {
            Table t = getConn().getTable(TableName.valueOf(tableName));
            //Hbase存储的是字节数组
            Put put = new Put(Bytes.toBytes(rowKey));
            put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier),
                    Bytes.toBytes(value));
            t.put(put);
            return true;
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            HBaseBasic02.close();
        }
        return false;
    }

    // 删除
    public static boolean del(String tableName, String rowKey, String family, String qualifier) {
        try {
            Table t = getConn().getTable(TableName.valueOf(tableName));
            Delete del = new Delete(Bytes.toBytes(rowKey));
            if (qualifier != null) {
                del.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
            } else if (family != null) {
                del.addFamily(Bytes.toBytes(family));
            }
            t.delete(del);
            return true;
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            HBaseBasic02.close();
        }
        return false;
    }

    //删除一行
    public static boolean del(String tableName, String rowKey) {
        return del(tableName, rowKey, null, null);
    }

    //删除一行下的一个列族
    public static boolean del(String tableName, String rowKey, String family) {
        return del(tableName, rowKey, family, null);
    }

    // 数据读取
    //取到一个值
    public static String byGet(String tableName, String rowKey, String family,
                               String qualifier) {
        try {
            Configuration conf = HBaseConfiguration.create(); // 获得配制文件对象;
            Connection conn = null;
            conf.set("hbase.zookeeper.quorum", "3.3.3.126,3.3.3.127,3.3.3.128,3.3.3.129,3.3.3.130");
            conf.set("hbase.zookeeper.property.clientPort", "2181");
            conf.set("ip", "http://chinallife-host10.sh2.newtouch.com");
            conf.set("port", "46177");
            try {
                conn = ConnectionFactory.createConnection(conf);// 获得连接对象
            } catch (IOException e) {
                e.printStackTrace();
            }

            Table t = getConn().getTable(TableName.valueOf(tableName));
//            Table t = conn.getTable(TableName.valueOf(tableName));
            Get get = new Get(Bytes.toBytes(rowKey));
//            get.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
            Result r = t.get(get);
            return Bytes.toString(CellUtil.cloneValue(r.listCells().get(0)));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    //取到一个族列的值
    public static Map<String, String> byGet(String tableName, String rowKey, String family) {
        Map<String, String> result = null;
        try {
            Configuration conf = HBaseConfiguration.create(); // 获得配制文件对象;
            Connection conn = null;
            conf.set("hbase.zookeeper.quorum", "3.3.3.126,3.3.3.127,3.3.3.128,3.3.3.129,3.3.3.130");
//            conf.set("hbase.zookeeper.quorum", "chinallife-host10.sh2.newtouch.com");
            conf.set("hbase.zookeeper.property.clientPort", "2181");
//            conf.set("ip", "chinallife-host10.sh2.newtouch.com");
//            conf.set("port", "46177");
            try {
                conn = ConnectionFactory.createConnection(conf);// 获得连接对象
            } catch (IOException e) {
                e.printStackTrace();
            }

//            Table t = getConn().getTable(TableName.valueOf(tableName));
            Table t = conn.getTable(TableName.valueOf(tableName));
            Get get = new Get(Bytes.toBytes(rowKey));
            get.addFamily(Bytes.toBytes(family));
            Result r = t.get(get);
            List<Cell> cs = r.listCells();
            result = cs.size() > 0 ? new HashMap<String, String>() : result;
            for (Cell cell : cs) {
                result.put(Bytes.toString(CellUtil.cloneQualifier(cell)), Bytes.toString(CellUtil.cloneValue(cell)));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;
    }

    //取到多个族列的值
    public static Map<String, Map<String, String>> byGet(String tableName, String rowKey) {
        Map<String, Map<String, String>> results = null;
        try {
            Table t = getConn().getTable(TableName.valueOf(tableName));
            Get get = new Get(Bytes.toBytes(rowKey));
            Result r = t.get(get);
            List<Cell> cs = r.listCells();
            results = cs.size() > 0 ? new HashMap<String, Map<String, String>>() : results;
            for (Cell cell : cs) {
                String familyName = Bytes.toString(CellUtil.cloneFamily(cell));
                if (results.get(familyName) == null) {
                    results.put(familyName, new HashMap<String, String>());
                }
                results.get(familyName).put(Bytes.toString(CellUtil.cloneQualifier(cell)), Bytes.toString(CellUtil.cloneValue(cell)));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return results;
    }

    public static void main(String[] args) {
//        Map<String, String> map = byGet("communication:contact_list", "91394137651_2324644334146560", "c");
        Map<String, String> map = byGet(args[0], args[1], args[2]);
        for (Map.Entry<String,String> entry : map.entrySet()){
            System.out.println("key::::::::::"+entry.getKey());
            System.out.println("value::::::::::"+entry.getValue());
        }
    }

}
