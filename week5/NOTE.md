```scala
8.15work
1.使用rdd api实现实现带词频的倒排索引
倒排索引(Inverted index),也称为反向索引。它是文档检索系统中常用的数据结构。被广泛应用与全文搜索引擎。
例子如下，被索引的文件为(0, 1, 2代表文件名)
0. "it is what it is"
1. "what is it"
2. "it is a banana"
我们就能得到下面的反向文件索引：
"a": {2}
"banana": {2}
"is": {0, 1, 2}
"it": {0, 1, 2}
"what": {0, 1}
再加上词频为：
"a": {(2, 1)}
"banana": {(2, 1)}
"is": {(0,2), (1,1), (2,1)}
"it": {(0,2), (1,1), (2,1)}
"what": {(0,1), (1,1)}

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

运行结果：
(a,CompactBuffer((2,1)))
(banana,CompactBuffer((2,1)))
(is,CompactBuffer((2,1), (1,1), (0,2)))
(it,CompactBuffer((1,1), (0,2), (2,1)))
(what,CompactBuffer((0,1), (1,1)))
```



```scala
2.distcp的spark实现
使用spark实现Hadoop分布式数据传输工具DistCp(distributed copy),只要求实现最基础的copy功能，对于-update、-diff、-p不做要求
对于HadoopDistCp的功能和实现，可以参考
https://hadoop.apache.org/docs/current/hadoop-distcp/DistCp.html
https://github.com/apache/hadoop/tree/release-2.7.1/hadoop-tools/hadoop-distcp
Hadoop使用MapReduce框架来实现分布式copy，在Spark中应使用RDD来实现分布式copy应实现的功能为：
sparkDistCp hdfs://xxx/source hdfs://xx/target
得到的结果为， 启动多个task/executor，将hdfs://xxx/source目录复制到hdfs://xxx/target，得到hdfs://xxx/target/source
需要支持source下存在多级子目录
需支持-i Ignore failures参数
需支持-m max concurrence 参数，控制同时copy的最大并发数
import org.apache.spark.{SparkConf, SparkContext}
import spire.std.string
import org.apache.hadoop.conf.Configuration

import scala.collection.mutable.ListBuffer
import org.apache.hadoop.fs.{FileSystem, Path}

object DistCpOps {
    def main(args: Array[String]): Unit = {
        type opsMap = Map[Symbol, Any]
        if (args.length == 0) print("abcde")
            val argList = args.toList

            def nextOption(map: opsMap, list: List[String]) : opsMap = {
                def isSwitch(s: String) = (s(0) == '-')

                list match {
                    case Nil => map
                    case "-i" :: value => nextOption(map ++ Map('ignoreFailures -> 1), list.tail)
                    case "-m" :: value :: tail =>
                        nextOption(map ++ Map('maxconcurrency -> value.toInt), tail)
                    case String :: Nil => nextOption(map ++ Map('outfile -> string), list.tail)
                    case String :: tail => nextOption(map ++ Map('infile -> string), tail)
                    case option :: opt2 :: tail if isSwitch(opt2) =>
                        println("Unknown option" + option)
                        sys.exit(1)
                }
            }
        val options = nextOption(Map(), argList)
        println(options)

        val sourceFolder = String.valueOf(options(Symbol("infile")))
        val targetFolder = String.valueOf(options(Symbol("outfile")))
        val concurrency = (options(Symbol("maxconcurrency"))).toString.toInt
        val ignoreFailure = options(Symbol("ignoreFailure")).toString.toInt

        val sparkConf = new SparkConf().setAppName("distCpOps").setMaster("master")
        val sc = new SparkContext(sparkConf)
        val sb = new StringBuffer()
        var fileNames = new ListBuffer[String]()

        val conf = new Configuration()
        conf.set("fs.defaultFS", "hdfs://localhost:9000")

        traverseDir(conf, sourceFolder, fileNames)
        fileNames.foreach(
            fileName =>
                try {
                    sc.textFile(fileName, concurrency).saveAsTextFile(fileName.replace(sourceFolder, targetFolder))
                } catch {
                    case t: Throwable => t.printStackTrace()
                        if (ignoreFailure == 0) {
                            throw new Exception("failed to copy " + fileName)
                        }
                })
        def traverseDir(conf: Configuration, path: String, filePaths: ListBuffer[String]): Unit = {
            val files = FileSystem.get(conf).listStatus(new Path(path))
            files.foreach {
                fSatus =>
                    {
                        if (!fSatus.isDirectory) {
                            filePaths += fSatus.getPath.toString
                        } else if (fSatus.isDirectory) {
                            traverseDir(conf, fSatus.getPath.toString, filePaths)
                        }
                    }
            }
        }
    }
}
```

