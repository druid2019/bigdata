import org.apache.spark.{SparkConf, SparkContext}

object InvertedCounts {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("invertedExamples").setMaster("local")
        val sc = new SparkContext(conf)
        val wordsRDD = sc.textFile("D:/words.txt")
            .flatMap {
                line =>
                    val array = line.split("\\.", 2)
                    val name = array(0)
                    array(1).split("\"")(1).trim.split(" ").map(word => (name, word))
            }

        val resultRDD = wordsRDD.map(k => (k._2, k._1)).map((_, 1L))
            .reduceByKey((x, y) => x + y)
            .map{case ((k, v), cnt) => (k, (v, cnt))}
            .groupByKey()
            .sortByKey()
            .collect()
            .foreach(println)
    }
}
