```scala
8.29日homework
1.为sparksql添加一条自定义命令
show version;
显示当前Spark版本和Java版本
编译环境：spark版本为tag = v3.1.2, Java版本为1.8 主要修改如下：
1)修改SqlBase.g4
spark/sql/catalyst/src/main/antlr4/org/apache/spark/sql/catalyst/parser/SqlBase.g4 
2)编写ShowVersionCommand方法
spark/sql/core/src/main/scala/org/apache/spark/sql/execution/command/ShowVersionCommand.scala 
3)sparksql中引用ShowVersionCommand方法
spark/sql/core/src/main/scala/org/apache/spark/sql/execution/SparkSqlParser.scala
详细见附件
```



```sql
2.构建sql满足如下要求
通过set spark.sql.planChangeLog.level=WARN;查看
1)构建一条SQL,同时apply下面三条优化规则：
CombineFilters
CollapseProject
BooleanSimplification
a.建表：
create table if not exists snap_face(rowkey int, name string, human_id string) partitioned by (day int);
create table if not exists snap_face(rowkey int, name string, human_id string) partitioned by (day int) STORED AS Parquet;

b.插入数据：
with tmp as (
select 1 as rowkey, '张三' as name, 'zs' as human_id
)insert overwrite table snap_face PARTITION (day=20210908) select rowkey, name, human_id from tmp;

with tmp as (
select 1 as rowkey, '张三' as name, 'zs' as human_id
union all
select 2 as rowkey, '李四' as name, 'ls' as human_id
union all
select 3 as rowkey, '王五' as name, 'ww' as human_id
)insert overwrite table snap_face PARTITION (day=20210908) select rowkey, name, human_id from tmp;

with tmp as (
select 4 as rowkey, '赵六' as name, 'zl' as human_id
)insert overwrite table snap_face PARTITION (day=20210909) select rowkey, name, human_id from tmp;

c.开启日志
set spark.sql.planChangeLog.level=WARN;

d.详细结果在q2-1result.txt里
> CombineFilters, 在Inferring Filters的时候做完了, line149
-- 逻辑计划规则，针对 Expect SELECT 进行转换
21/09/09 13:56:05 WARN [main] PlanChangeLogger: 
=== Result of Batch Operator Optimization before Inferring Filters ===
 Project [name#25]                                                    Project [name#25]
!+- Filter (true AND (day#27 = 20210908))                             +- Filter ((day#27 < 20210909) AND (day#27 = 20210908))
!   +- Project [rowkey#24, name#25, human_id#26, day#27]                 +- Relation[rowkey#24,name#25,human_id#26,day#27] parquet
!      +- Filter (day#27 < 20210909)                                  
!         +- Relation[rowkey#24,name#25,human_id#26,day#27] parquet   

> CollapseProject, Line 136 
-- 逻辑计划规则，对相同列有多个 Project 时进行合并
21/09/09 13:56:05 WARN [main] PlanChangeLogger: 
=== Applying Rule org.apache.spark.sql.catalyst.optimizer.CollapseProject ===
 Project [name#25]                                                       Project [name#25]
!+- Project [name#25]                                                    +- Filter ((day#27 < 20210909) AND (true AND (day#27 = 20210908)))
!   +- Filter ((day#27 < 20210909) AND (true AND (day#27 = 20210908)))      +- Relation[rowkey#24,name#25,human_id#26,day#27] parquet
!      +- Relation[rowkey#24,name#25,human_id#26,day#27] parquet

> BooleanSimplification, line 143
-- 逻辑计划规则，消除无意义的过滤规则
21/09/09 13:56:05 WARN [main] PlanChangeLogger: 
=== Applying Rule org.apache.spark.sql.catalyst.optimizer.BooleanSimplification ===
 Project [name#25]                                                    Project [name#25]
!+- Filter ((day#27 < 20210909) AND (true AND (day#27 = 20210908)))   +- Filter ((day#27 < 20210909) AND (day#27 = 20210908))
    +- Relation[rowkey#24,name#25,human_id#26,day#27] parquet            +- Relation[rowkey#24,name#25,human_id#26,day#27] parquet

2)构建一条SQL,同时apply下面五条优化规则：
ConstantFolding
PushDownPredicates
ReplaceDistinctWithAggregate
ReplaceExceptWithAntiJoin
FoldablePropagation
详细结果在q2-2result.txt里面，详细结果如下：
> ConstantFolding, Line 291
-- 逻辑计划规则，常量折叠
21/09/09 14:38:30 WARN [main] PlanChangeLogger: 
=== Applying Rule org.apache.spark.sql.catalyst.optimizer.ConstantFolding ===
 Sort [5 ASC NULLS FIRST], true                                                              Sort [5 ASC NULLS FIRST], true
 +- Aggregate [rowkey#27, name#28, human_id#29, day#30], [rowkey#27, name#28, 5 AS num#26]   +- Aggregate [rowkey#27, name#28, human_id#29, day#30], [rowkey#27, name#28, 5 AS num#26]
!   +- Filter (NOT coalesce((20210908 = 20210909), false) AND (day#30 = 20210908))              +- Filter (true AND (day#30 = 20210908))
       +- Project [rowkey#27, name#28, human_id#29, day#30]                                        +- Project [rowkey#27, name#28, human_id#29, day#30]
!         +- Filter (1 = 1)                                                                           +- Filter true
             +- Relation[rowkey#27,name#28,human_id#29,day#30] parquet                                   +- Relation[rowkey#27,name#28,human_id#29,day#30] parquet
             
> PushDownPredicates, Line 242
-- 逻辑计划规则，谓词下推
21/09/09 14:38:30 WARN [main] PlanChangeLogger: 
=== Applying Rule org.apache.spark.sql.catalyst.optimizer.PushDownPredicates ===
 Sort [num#26 ASC NULLS FIRST], true                                                                       Sort [num#26 ASC NULLS FIRST], true
 +- Project [rowkey#27, name#28, 5 AS num#26]                                                              +- Project [rowkey#27, name#28, 5 AS num#26]
!   +- Filter (day#30 = 20210908)                                                                             +- Aggregate [rowkey#27, name#28, human_id#29, day#30], [rowkey#27, name#28, human_id#29, day#30]
!      +- Aggregate [rowkey#27, name#28, human_id#29, day#30], [rowkey#27, name#28, human_id#29, day#30]         +- Filter (NOT coalesce((day#30 = 20210909), false) AND (day#30 = 20210908))
!         +- Filter NOT coalesce((day#30 = 20210909), false)                                                        +- Project [rowkey#27, name#28, human_id#29, day#30]
!            +- Project [rowkey#27, name#28, human_id#29, day#30]                                                      +- Filter (1 = 1)
!               +- Filter (1 = 1)                                                                                         +- Relation[rowkey#27,name#28,human_id#29,day#30] parquet
!                  +- Relation[rowkey#27,name#28,human_id#29,day#30] parquet

> ReplaceDistinctWithAggregate, Line 217
-- 逻辑计划规则，将 Dinstinct 替换为 Aggregate 操作
21/09/09 14:38:30 WARN [main] PlanChangeLogger: 
=== Applying Rule org.apache.spark.sql.catalyst.optimizer.ReplaceDistinctWithAggregate ===
 Sort [num#26 ASC NULLS FIRST], true                                           Sort [num#26 ASC NULLS FIRST], true
 +- Project [rowkey#27, name#28, 5 AS num#26]                                  +- Project [rowkey#27, name#28, 5 AS num#26]
    +- Filter (day#30 = 20210908)                                                 +- Filter (day#30 = 20210908)
!      +- Distinct                                                                   +- Aggregate [rowkey#27, name#28, human_id#29, day#30], [rowkey#27, name#28, human_id#29, day#30]
          +- Filter NOT coalesce((day#30 = 20210909), false)                            +- Filter NOT coalesce((day#30 = 20210909), false)
             +- Project [rowkey#27, name#28, human_id#29, day#30]                          +- Project [rowkey#27, name#28, human_id#29, day#30]
                +- Filter (1 = 1)                                                             +- Filter (1 = 1)
                   +- Relation[rowkey#27,name#28,human_id#29,day#30] parquet                     +- Relation[rowkey#27,name#28,human_id#29,day#30] parquet
                   
> FoldablePropagation, Line 282
-- 逻辑计划规则，尽可能的将属性名替换为可折叠表达式
21/09/09 14:38:30 WARN [main] PlanChangeLogger: 
=== Applying Rule org.apache.spark.sql.catalyst.optimizer.FoldablePropagation ===
!Sort [num#26 ASC NULLS FIRST], true                                                         Sort [5 ASC NULLS FIRST], true
 +- Aggregate [rowkey#27, name#28, human_id#29, day#30], [rowkey#27, name#28, 5 AS num#26]   +- Aggregate [rowkey#27, name#28, human_id#29, day#30], [rowkey#27, name#28, 5 AS num#26]
    +- Filter (NOT coalesce((20210908 = 20210909), false) AND (day#30 = 20210908))              +- Filter (NOT coalesce((20210908 = 20210909), false) AND (day#30 = 20210908))
       +- Project [rowkey#27, name#28, human_id#29, day#30]                                        +- Project [rowkey#27, name#28, human_id#29, day#30]
          +- Filter (1 = 1)                                                                           +- Filter (1 = 1)
             +- Relation[rowkey#27,name#28,human_id#29,day#30] parquet                                   +- Relation[rowkey#27,name#28,human_id#29,day#30] parquet
             

```



```scala
3.实现自定义优化规则
第一步实现自定义规则（静默规则，通过set spark.sql.planChangeLog.level=WARN;确认执行到就行）
case class MyPushDown(spark:SparkSession) extends Rule[LogicalPlan] {
	def apply(plan:LogicalPlan):LogicalPlan = plan transform {...}
}
第二步 创建自己的Extension并注入
class MySparkSessionExtension extends(SparkSessionExtensions => Unit) {
    override def apply(extensions:SparkSessionExtensions):Unit = {
        extensions.injectOptimizerRule{session =>
        	new MyPushDown(session)
        }
    }
}
第三步 通过spark.sql.extensions提交
bin/sqark-sql --jars my.jar --conf spark.sql.extensions-com.jikeshijian.MySparkSessionExtension
CustomRule:
case class CustomRule(spark: SparkSession) extends Rule[LogicalPlan] with Logging {

  def apply(plan: LogicalPlan): LogicalPlan = {
    logWarning("进入自定义规则")
    plan transform {
      case project@Project(projectList, _) =>
        logWarning("匹配到 Project")
        projectList.foreach {
          name =>
            logWarning("字段名称:" + name)
        }
        project
    }
  }
}

CustomSparkSessionExtension
class CustomSparkSessionExtension extends (SparkSessionExtensions => Unit) with Logging {
  override def apply(extensions: SparkSessionExtensions): Unit = {
    extensions.injectOptimizerRule { session =>
      logWarning("进入自定义扩展")
      CustomRule(session)
    }
  }
}
```

