package com.github.tiger.spark.demo;

import com.github.tiger.hadoop.FileSystemClient;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.deploy.SparkSubmit;

import java.io.IOException;
import java.net.URISyntaxException;

/**
 * @author liuhongming
 */
public class SubmitJobToSpark {

    public static void main(String[] args) throws IOException, URISyntaxException {

        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://spark-220:9000");

        System.setProperty("HADOOP_USER_NAME", "root");

        String local = "../campus-public/spark/build/libs/campus-spark-1.0.0.jar";

        String dest = "/jars/campus-spark-1.0.jar";

        FileSystemClient.copyFileToHdfs(conf, local, dest);

        String[] mainArgs = new String[]{
                // 应用程序入口
                "--class", "com.zpcampus.util.demo.WordCount",
                // 集群 master URL
                "--master", "spark://spark-220:6066",
                // local（本地），cluster（集群）
                "--deploy-mode", "cluster",
                /**
                 * file: - 绝对路径和 file:/ URI 通过 driver 的 HTTP file server 提供服务，
                 * 并且每个 executor 会从 driver 的 HTTP server 拉取这些文件。
                 * hdfs:, http:, https:, ftp: - 如预期的一样拉取下载文件和 JAR
                 * local: - 一个用 local:/ 开头的 URL 预期作在每个 worker 节点上作为一个本地文件存在。
                 * 这样意味着没有网络 IO 发生，并且非常适用于那些已经被推送到每个 worker 或通过 NFS，
                 * GlusterFS等共享的大型的 file/JAR。
                */
                "--jars", "local:/data/jars/jedis-2.8.1.jar",
                // 应用包
                "hdfs://spark-220:9000/jars/campus-spark-1.0.jar",
                // 应用参数
                "hdfs://spark-220:9000/word_count",
                "hdfs://spark-220:9000/word_count_result"
        };

        SparkSubmit.main(mainArgs);
    }

}

