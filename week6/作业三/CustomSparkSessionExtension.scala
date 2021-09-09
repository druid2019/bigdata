import org.apache.spark.sql.{Logging, SparkSessionExtensions}

class CustomSparkSessionExtension extends (SparkSessionExtensions => Unit) with Logging {
    override def apply(extensions: SparkSessionExtensions): Unit = {
        extensions.injectOptimizerRule {
            session =>
                logWarning("进入自定义扩展")
                CustomRule(session)
        }
    }
}
