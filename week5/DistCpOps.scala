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
