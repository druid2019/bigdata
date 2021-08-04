package com.memo.hb;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;


public class HbaseClientUtil {

    public static Configuration conf;
    // 初始化操作
    static {
        // 1. 初始化配置
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "jikehadoop01,jikehadoop02");
        // hbase默认端口
//        conf.set("hbase.master", "16000");
        // zk默认端口
        conf.set("hbase.zookeeper.property.clientPort", "2181");
    }

    // 创建表
    public void creatTable() throws IOException {
        // 1. 初始化配置
//        Configuration conf = HBaseConfiguration.create();
//        conf.set("hbase.zookeeper.quorum", "jikehadoop01,jikehadoop02");
        // hbase默认端口
//        conf.set("hbase.master", "16000");
        // zk默认端口
//        conf.set("hbase.zookeeper.property.clientPort", "2181");
        // 2. 创建连接
        Connection conn = ConnectionFactory.createConnection(conf);
        // 3. 获得一个建表、删表对象hbaseAdmin()是继承admin()
        Admin admin = conn.getAdmin();
        // 4. 创建表的描述信息
        HTableDescriptor student = new HTableDescriptor(TableName.valueOf("jinyihao:student"));
        // 5. 添加列簇
        student.addFamily(new HColumnDescriptor("info"));
        student.addFamily(new HColumnDescriptor("score"));
        // 6. 调用api进行建表操作
        admin.createTable(student);
    }

    // 向表中插入数据
    public void insertTable() throws IOException {
        // 2. 创建连接
        Connection conn = ConnectionFactory.createConnection(conf);
        // 3.创建table表
        Table student = conn.getTable(TableName.valueOf("jinyihao:student"));
        // 4.创建Put类
        Put put = new Put(Bytes.toBytes("Tom"));
        // 5. 向Put中添加 列簇，列名，值 注意：需要转化成字节数组
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("student_id"),Bytes.toBytes("20210353150086"));
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("class"),Bytes.toBytes("1"));
        put.addColumn(Bytes.toBytes("score"), Bytes.toBytes("understanding"),Bytes.toBytes("75"));
        put.addColumn(Bytes.toBytes("score"), Bytes.toBytes("programming"),Bytes.toBytes("80"));

    }

}
