package com.github.tiger.spark.demo;

import org.apache.spark.deploy.SparkSubmit;

/**
 * @author liuhongming
 */
public class SubmitJobToSpark {

    public static void main(String[] args) {

        String workJar = "hdfs://spark-220:9000/jars/campus-spark-1.0.jar";
        String[] arg0 = new String[]{
                "--master", "spark://spark-220:6066",
                "--deploy-mode", "cluster",
                "--name", "test java submit job to spark",
                "--class", "spark.WordCount",
                workJar,
                "hdfs://spark-220:9000/word_count",
                "hdfs://spark-220:9000/word_count_result"
        };

        SparkSubmit.main(arg0);
    }

}

