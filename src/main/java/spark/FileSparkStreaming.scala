package spark

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hdfs.DistributedFileSystem
import org.apache.spark.deploy.SparkSubmit

object FileSparkStreaming {

  def main(args: Array[String]) {
    val localJar: String = "F:/Workspace@2016/spark-parent/target/spark-1.0.jar"
    val conf: Configuration = new Configuration
    val dfs: DistributedFileSystem = new DistributedFileSystem
    dfs.initialize(URI.create("hdfs://namenode:8020"), conf)
    dfs.copyFromLocalFile(false, true, new Path("f:/helloworld4"), new Path("/text"))
    dfs.close()

//    val hadoopJar: String = "hdfs://namenode:8020/spark-1.0.jar"
//    val arg0: Array[String] = Array[String](
//      "--class", "spark.FileWordCount",
//      "--master", "spark://192.168.20.215:7077",
//      "--packages", "org.apache.spark:spark-streaming-kafka_2.11:1.6.3",
//      "--repositories", "http://172.16.230.111:8081/nexus/content/groups/public",
//      hadoopJar)
//    SparkSubmit.main(arg0)
  }

}
