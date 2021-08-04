启动命令：
hadoop jar mpwordcount-1.0-SNAPSHOT.jar 输入文件 输出目录
hadoop jar mpwordcount-1.0-SNAPSHOT.jar jyh/mapreduce/input/HTTP_20130313143750.dat jyh/mapreduce/output

查看运行结果：
hdfs dfs -cat jyh/mapreduce/output/*

服务器地址
47.101.72.185
路径：
/home/student/jyh/mapreduce

hdfs dfs -rm -f 文件夹名称

