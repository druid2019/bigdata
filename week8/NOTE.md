```scala
作业1、写出5条HyperLogLog的用途或大数据场景下的实际案例。
统计实时在线人数或注册用户数
统计页面实时uv数
统计每日访问ip数
估计数据量级
在监控系统中使用
统计搜索页面不同词条的搜索数量
Redis结合HyperLogLog算法来近似统计数据去重数

作业2、在本地docker环境或阿里云e-mapreduce环境进行SQL查询，要求在Presto中使用HyperLogLog计算近似基数。
使用本地 Docker 搭建 Presto 的方式执行作业

拉取docker镜像
docker pull ahanaio/prestodb-sandbox
启动容器
docker run -p 8080:8080 --name presto ahanaio/prestodb-sandbox
连接presto客户端
docker exec -it presto presto-cli
presto:sf100> select approx_distinct(c_customer_sk) from customer;
  _col0
---------
 1982709
(1 row)

Query 20211007_134217_00027_wy7qq, FINISHED, 1 node
Splits: 23 total, 23 done (100.00%)
0:08 [2M rows, 0B] [245K rows/s, 0B/s]

作业3、学习使用Presto-Jdbc库连接docker或e-mapreduce环境，重复上述查询。
代码
package org.presto;

import java.sql.*;

public class PrestoJdbcUtil {
    public static void main(String[] args) throws SQLException {
        String url = "jdbc:presto://localhost:8080/tpcds/sf100";
        Connection connection = DriverManager.getConnection(url, "test", null);

        PreparedStatement preparedStatement = connection
                .prepareStatement("select approx_distinct(c_customer_sk) from customer");
        ResultSet rs = preparedStatement.executeQuery();
        while(rs.next()){
            System.out.println(rs.getString(1));
        }
    }
}
```

