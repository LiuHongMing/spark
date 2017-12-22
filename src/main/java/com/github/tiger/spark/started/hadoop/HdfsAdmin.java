package com.github.tiger.spark.started.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;

import java.io.IOException;
import java.net.URI;

public class HdfsAdmin {

    public void resolvHdfs() throws IOException {
        String resourcePath = Thread.currentThread().getContextClassLoader().getResource("").getPath();
        String localJar = resourcePath + "people.json";
        Configuration conf = new Configuration();
        DistributedFileSystem dfs = new DistributedFileSystem();
        dfs.initialize(URI.create("hdfs://175.63.101.122:9000"), conf);
        dfs.copyFromLocalFile(false, true, new Path(localJar), new Path("/test/people.json"));
        dfs.close();
    }

    public static void main(String[] args) throws IOException {
        HdfsAdmin admin = new HdfsAdmin();
        admin.resolvHdfs();
    }

}
