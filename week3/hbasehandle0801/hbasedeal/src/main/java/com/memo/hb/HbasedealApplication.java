package com.memo.hb;

import com.sun.org.apache.xml.internal.utils.NameSpace;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@SpringBootApplication(exclude= {DataSourceAutoConfiguration.class})
public class HbasedealApplication {

    /**
     *  初始化hbase
     */
    public static Connection getHbaseConnection() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "jikehadoop01,jikehadoop02");
        // hbase默认端口
//        conf.set("hbase.master", "16000");
        // zk默认端口
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        // 2. 创建连接
        return ConnectionFactory.createConnection(conf);
    }

    /**
     *  创建命名空间
     */
    public static void createNameSpace() throws IOException {
        System.out.println("开始创建命名空间-------");
        Connection conn = getHbaseConnection();
        // 3. 获得一个建表、删表对象hbaseAdmin()是继承admin()
        Admin admin = conn.getAdmin();
        // 创建命名空间
        NamespaceDescriptor.Builder nsdesc = NamespaceDescriptor.create("jinyihao");
        // 设置描述
        nsdesc.addConfiguration("jinyihao", "nlname");
        NamespaceDescriptor build = nsdesc.build();
        admin.createNamespace(build);
        admin.close();
        System.out.println("已创建命名空间-------");
    }

    /*
    * 删除表
    * */
    public void dropTable() throws IOException {
        System.out.println("开始drop表了-------");
        Connection conn = getHbaseConnection();
        // 3. 获得一个建表、删表对象hbaseAdmin()是继承admin()
        Admin admin = conn.getAdmin();
        // 4. 调用api禁用表
        admin.disableTable(TableName.valueOf("jinyihao:student"));
        // 5. 调用api删除表
        admin.deleteTable(TableName.valueOf("jinyihao:student"));
    }

    /**
     *  创建表
     */
    public static void createTable() throws IOException {
        System.out.println("开始创建表了-------");
        Connection conn = getHbaseConnection();
        // 3. 获得一个建表、删表对象hbaseAdmin()是继承admin()
        Admin admin = conn.getAdmin();
        // 4. 创建表的描述信息
        HTableDescriptor student = new HTableDescriptor(TableName.valueOf("jinyihao:student"));
        // 5. 添加列簇
        student.addFamily(new HColumnDescriptor("info"));
        student.addFamily(new HColumnDescriptor("score"));
        // 6. 调用api进行建表操作
        admin.createTable(student);
        System.out.println("结束创建-------");
    }

    /**
     *  插入数据
     */
    public static void insertTable() throws IOException {
        System.out.println("开始插入数据-------");
        // 2. 创建连接
        Connection conn = getHbaseConnection();
        // 3.创建table表
        Table student = conn.getTable(TableName.valueOf("jinyihao:student"));
        // 4.创建Put类
        Put put = new Put(Bytes.toBytes("Tom"));
        // 5. 向Put中添加 列簇，列名，值 注意：需要转化成字节数组
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("student_id"),Bytes.toBytes("20210353150086"));
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("class"),Bytes.toBytes("1"));
        put.addColumn(Bytes.toBytes("score"), Bytes.toBytes("understanding"),Bytes.toBytes("75"));
        put.addColumn(Bytes.toBytes("score"), Bytes.toBytes("programming"),Bytes.toBytes("80"));
        // 6. 调用api插入数据
        student.put(put);
        System.out.println("插入数据完毕-------");
    }

    /**
     *  查询数据
     */
    public static void searchTable() throws IOException {
        // 2. 创建连接
        Connection conn = getHbaseConnection();
        // 3.创建table表
        Table student = conn.getTable(TableName.valueOf("jinyihao:student"));
        // 4. 创建 Get 类
        Get get = new Get(Bytes.toBytes("Tom"));
        // 5.调用API进行获取数据
        Result result = student.get(get);
        // 6. 将返回的结果进行遍历输出
        Cell[] cells = result.rawCells();
        for (Cell cell : cells) {
            System.out.println("rowkey ："+Bytes.toString(CellUtil.cloneRow(cell)));
            System.out.println("列簇 ："+Bytes.toString(CellUtil.cloneFamily(cell)));
            System.out.println("列名 ："+Bytes.toString(CellUtil.cloneQualifier(cell)));
            System.out.println("值 ："+Bytes.toString(CellUtil.cloneValue(cell)));
            System.out.println("----------------");
        }
    }

    /**
     *  删除数据
     */
    public static void deleteRowKey(String tableName, String rowKey) throws IOException {
        // 2. 创建连接
        Connection conn = getHbaseConnection();
        Table table = conn.getTable(TableName.valueOf(tableName));
        Delete delete = new Delete(rowKey.getBytes(rowKey));
        List<Delete> deleteList = new ArrayList<>();
        deleteList.add(delete);
        table.delete(deleteList);
    }

    public static void main(String[] args) throws IOException {
        SpringApplication.run(HbasedealApplication.class, args);

        // 创建命名空间
//        createNameSpace();

        // 创建表操作
        createTable();


        // 插入数据
        insertTable();

        // 查询数据
        searchTable();

        // 删除数据
//        deleteRowKey("jinyihao:student", "Tom");
    }

}
