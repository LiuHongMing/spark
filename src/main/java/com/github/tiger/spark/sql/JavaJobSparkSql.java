package com.github.tiger.spark.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.StreamingContext;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @author liuhongming
 */
public class JavaJobSparkSql {

    public static void main(String[] args) throws AnalysisException, IOException {
        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local[*]").setAppName("Job aggregration");
        SparkContext sc = new SparkContext(sparkConf);

        StreamingContext ssc = new StreamingContext(sc, new Duration(10));
//        ssc.socketTextStream("localhost", 5200,
//                StorageLevel.MEMORY_AND_DISK());

        SparkSession spark = SparkSession
                .builder()
                .config(sparkConf)
                .getOrCreate();
        runBasicDataFrame(spark);

        ssc.awaitTermination();

    }

    private static void runBasicDataFrame(SparkSession spark) {
        String sample = Thread.currentThread()
                .getContextClassLoader()
                .getResource("job.json")
                .getPath();
        long start = System.currentTimeMillis();
        Dataset<Row> df = spark.read().json(sample);
        long records = df.count();
        df.createOrReplaceTempView("job");
        Dataset<Row> sqlDF = spark.sql("SELECT ID, JOB_TYPE, CITY_ID, JOB_NATURE, count(1) " +
                "FROM job group by ID, JOB_TYPE, CITY_ID, JOB_NATURE");
        long elasped = System.currentTimeMillis() - start;
        System.out.println("records：" + records + "，aggregation time：" + TimeUnit.MILLISECONDS.toSeconds(elasped) + "s");
        List<Row> result = sqlDF.javaRDD().collect();
        int total = 0;
        for (Row row : result) {
            total += row.toString().getBytes().length;
        }
        System.out.println((double) total / (1024 * 1024));
    }

}
