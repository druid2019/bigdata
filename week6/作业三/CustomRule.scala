import org.apache.spark.sql.{Logging, SparkSession}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule

case class CustomRule(spark: SparkSession) extends Rule[LogicalPlan] with Logging {
    def apply(plan: LogicalPlan): LogicalPlan = {
        logWarning("进入自定义规则")
        plan transform {
            case project@Project(projectList, _) =>
                logWarning("匹配到Project")
                projectList.foreach {
                    name =>
                        logWarning("字段名称：" + name)
                }
                project
        }
    }
}
