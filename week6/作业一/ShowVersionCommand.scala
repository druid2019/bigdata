import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.SparkContext
import org.apache.spark.sql.execution.command.RunnableCommand

import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.types.StringType

case class ShowVersionCommand() extends RunnableCommand{
    override val output: Seq[Attribute] =
        Seq(AttributeReference("version", StringType, nullable = true)())

    override def run(sparkSession: SparkSession): Seq[Row] = {
        val sc: SparkContext = SparkContext.getOrCreate()
        // 打印spark和Java的版本
        val outputString = "Spark Version: " + sc.version + " Java Version: " +
        System.getProperty("java.runtime.version")
        Seq(Row(outputString))
    }
}
