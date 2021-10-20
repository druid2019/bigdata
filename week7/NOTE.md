```scala
1.思考：如何避免小文件问题
如何避免小文件问题？给出2~3种解决方案
HDFS中小文件是指文件size小于HDFS上block(dfs.block.size)大小的文件，大量的小文件会给Hadoop的扩展性和性能带来严重的影响。
小文件是如何产生的？
1) 动态分区插入数据，产生大量的小文件，从而导致map数量剧增。
2) reduce数量越多，小文件也越多，reduce的个数和输出文件个数一致。
3) 数据源本身就是大量的小文件。
小文件问题的影响
从mapreduce的角度看，一个文件会启动一个map，所以小文件越多，map越多，一个map启动一个jvm去执行，所以这些任务的初始化、启动、执行会浪费大量的资源，严重影响性能。

从HDFS角度看，HDFS中文件元信息(位置，大小，分块等)保存在NameNode的内存中，每个对象大约占用150byte(字节)，如果小文件过多，会占用大量内存，直接影响NameNode的性能；HDFS读写小文件也会更加耗时，每次都需要从NameNode获取元信息，并于对应的DataNode建立连接。

如何解决小文件问题：
a) 输入合并，在Map前合并小文件
b) 输出合并，在输出结果的时候合并小文件
c) 控制reduce个数来实现减少小文件个数
d) 通过repartition或coalesce算子控制最后的DataSet分区数
e) 定期使用异步方式合并小文件
f) 将hive风格的Coalesce and Repartition Hint应用到Spark SQL，如INSERT...SELECT COALESCE/REPARTITION(numPartitions)
g) 配置spark.sql.shuffle.partitions = 1, 同时为了避免数据倾斜，insert sql后面可以加上distribute by rand()

2.实现Compact table command
.要求：
添加compact table命令，用于合并小文件，例如表test1总共有50000个文件，每个1MB，通过该命令，合称为500个文件，每个约100MB。
.语法：
COMPACT TABLE table_identify [partitionSpec][INFO fileNum FILES];
.说明：
1.如果添加partitionSpec，则只合并指定的partition目录的文件。
2.如果不加into fileNum files,则把表中的文件合并成128MB大小。
3.以上两个算附加要求，基本要求只需要完成以下功能：
COMPACT TABLE test1 INTO 500 FILES;
代码参考：
SqlBase.g4:
|COMPACT TABLE target=tableIdentifier partitionSpec?
  (INTO fileNum=INTEGER_VALUE identifier)?
a).在SqlBase.g4中添加：statement添加
COMPACT TABLE tableIdentifier partitionSpec? (INTO INTEGER_VALUE FILES) ? 
nonReserved添加
FILES
ansiNonReserved添加
FILES
keywords list添加：
FILES: 'FILES'
b)使用antlr4插件自动生成代码
c) 添加代码
override def visitCompactTable(ctx: CompactTableContext): LogicalPlan = withOrigin(ctx) {
    val table: TableIdentifier = visitTableIdentifier(ctx.tableIdentifier())
    
    val fileNumStr: String = ctx.INTEGER_VALUE().getText
    val fileNum: Option[Int] = if (fileNumStr == null) {
        None
    } else {
        try {
            Some(fileNumStr.toInt)
        } catch {
            case e: Exception =>
                throw new IllegalArgumentException(s"files num should be Int, but is ${fileNumStr}.")
        }
    }
    CompactTableCommand(table, fileNum)
}
d) 添加类org.apache.spark.sql.execution.command.CompactTableCommand
case class CompactTableCommand(table: TableIdentifier, fileNum: Option[Int]) extends RunnableCommand {
    private val tmpTable: TableIdentifier = TableIdentifier("??")
    
    override def output: Seq[Attribute] = Seq(AttributeReference("no_return"), StringType, false)())
    override def run(spark: SparkSession): Seq[Row] = {
        val dataDF: DataFrame = spark.table(table)
        	val num: Int = fileNum match {
                case Some(i) => i
                case _ =>
                  (spark.sessionState
                        .executePlan(dataDF.queryExecution.logical)
                        .optimizedPlan
                        .stats.sizeInBytes / (1024L * 1024L * 128L)
                  ).toInt
            }
        dataDF.repartition(num).write.mode(SaveMode.Overwrite).saveAsTable(tmpTable.identifier)
        spark.table(tmpTable.identifier).write.mode(SaveMode.Overwrite).saveAsTable(table.identifier)
        spark.table(s"drop table ${tmpTable.identifier}")
        log.warn("Compacte Table Completed.")
        Seq()
    }
}

3.Insert命令自动合并小文件
.AQE可以自动调整reducer的个数，但是正常跑Insert命令不会自动合并小文件，例如insert into t1 select * from t2;
.请加一条物理规则(Strategy),让Insert命令自动进行小文件合并(repartition)。(不用考虑bucket表，不用考虑Hive表)

代码参考
object RepartitionForInsertion extends Rule[SparkPlan] {
	override def apply(plan:SparkPlan):SparkPlan = {
		plan transformDown {
			case i @ InsertIntoDataSourceExec(child,_,_,partitionColumns,_)
			...
			val newChild = ...
			i.withNewChildren(newChild:Nil)
		}
	}
}
```

