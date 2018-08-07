package com.github.tiger.spark.streaming.kafka;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.spark.deploy.SparkSubmit;

import java.io.IOException;
import java.net.URI;

public class KafkaSparkStreaming {

    public static void main(String[] args) throws IOException {
        String localJar = "F:/Workspace@2016/spark-parent/target/spark-1.0.jar";
        Configuration conf = new Configuration();
        DistributedFileSystem dfs = new DistributedFileSystem();
        dfs.initialize(URI.create("hdfs://namenode:8020"), conf);
        dfs.copyFromLocalFile(false, true, new Path(localJar), new Path("/spark-1.0.jar"));
        dfs.close();

        String hadoopJar = "hdfs://namenode:8020/spark-1.0.jar";
        String[] arg0 = new String[]{
                "--class", "spark.KafkaWordCount",
                "--master", "spark://192.168.20.215:7077",
                "--packages", "org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.2",
                "--repositories", "http://172.16.230.111:8081/nexus/content/groups/public",
                hadoopJar
        };
        SparkSubmit.main(arg0);
    }

}
