package spark;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.spark.deploy.SparkSubmit;

import java.io.IOException;
import java.net.URI;

public class SubmitJobToSpark {

    public static void main(String[] args) throws IOException {

        String localJar = "F:/Workspace@2016/spark-parent/target/spark-1.0.jar";
        Configuration conf = new Configuration();
        DistributedFileSystem dfs = new DistributedFileSystem();
        dfs.initialize(URI.create("hdfs://namenode:8020"), conf);
        dfs.copyFromLocalFile(false, true, new Path(localJar), new Path("/spark-1.0.jar"));
        dfs.delete(new Path("/wordcount"), true);
        dfs.close();

        String hadoopJar = "hdfs://namenode:8020/spark-1.0.jar";
        String[] arg0 = new String[]{
                "--master", "spark://192.168.20.215:7077",
                "--deploy-mode", "client",
                "--name", "test java submit job to spark",
                "--class", "spark.WordCount",
                hadoopJar,
                "hdfs://namenode:8020/README.md",
                "hdfs://namenode:8020/wordcount"
        };
        SparkSubmit.main(arg0);
    }

}

