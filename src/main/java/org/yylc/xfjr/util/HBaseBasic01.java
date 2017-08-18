package org.yylc.xfjr.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;

//import org.apache.hadoop.hbase.io.BatchUpdate;

public class HBaseBasic01 {
    static HBaseConfiguration hbaseConfig=null;
    static{  
        Configuration config=new Configuration();
        config.set("hbase.zookeeper.quorum","192.168.10.149,192.168.10.44,192.168.10.49");  
        config.set("hbase.zookeeper.property.clientPort", "2181");  
        hbaseConfig=new HBaseConfiguration(config);  
    }  
      
    /** 
     * 创建一张表 
     * @throws IOException  
     */  
    public static void createTable(String tablename) throws IOException{  
        HBaseAdmin admin = new HBaseAdmin(hbaseConfig);  
        if(admin.tableExists(tablename)){  
            System.out.println("table Exists!!!");  
        }else{  
            HTableDescriptor tableDesc = new HTableDescriptor(tablename);  
            tableDesc.addFamily(new HColumnDescriptor("name:"));  
            admin.createTable(tableDesc);  
            System.out.println("create table ok.");  
        }  
    }  
      
    /** 
     * 删除一张表 
     * @throws IOException  
     */  
    public static void dropTable(String tablename) throws IOException{  
        HBaseAdmin admin = new HBaseAdmin(hbaseConfig);  
        admin.disableTable(tablename);  
        admin.deleteTable(tablename);  
        System.out.println("drop table ok.");  
    }  
      
      
    /** 
     * 添加一条数据 
     * @throws IOException  
     */  
    public static void addData(String tablename) throws IOException{  
        HTable table=new HTable(hbaseConfig,tablename);  
//        BatchUpdate update=new BatchUpdate("Huangyi");
//        update.put("name:java","http://www.sun.com".getBytes());
//        table.commit(update);
        System.out.println("add data ok.");  
    }  
      
      
    /** 
     * 显示所有数据 
     * @throws IOException  
     */  
    public static void getAllData(String tablename) throws IOException {
        HTable table=new HTable(hbaseConfig,tablename);  
        Scan s=new Scan();  
        ResultScanner rs=table.getScanner(s);  
        for(Result r:rs){  
            for(KeyValue kv:r.raw()){  
                System.out.println("rowkey : "+new String(kv.getRow()));  
//                System.out.println(new String(kv.getColumn())+" = "+new String(kv.getValue()));
            }  
        }  
    }  
      
    public static void main(String [] args) throws IOException{  
            String tablename="table_1";  
            HBaseBasic01.createTable(tablename);
            HBaseBasic01.addData(tablename);
            HBaseBasic01.getAllData(tablename);
            HBaseBasic01.dropTable(tablename);
    }  
      
      
}  