package particular;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.StreamingContext;

import java.io.*;
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

        ssc.socketTextStream("localhost", 5200,
                StorageLevel.MEMORY_AND_DISK());

        SparkSession spark = SparkSession
                .builder()
                .config(sparkConf)
                .getOrCreate();
        runBasicDataFrame(spark);

        ssc.awaitTermination();

    }

    private static void runBasicDataFrame(SparkSession spark) {
        String cp = Thread.currentThread().getContextClassLoader().getResource("").getPath();
        String sample = cp + "job.json";
        long start = System.currentTimeMillis();
        Dataset<Row> df = spark.read().json(sample);
        df.createOrReplaceTempView("job");
        Dataset<Row> sqlDF = spark.sql("SELECT ID, JOB_TYPE, CITY_ID, JOB_NATURE, count(1) " +
                "FROM job group by ID, JOB_TYPE, CITY_ID, JOB_NATURE");
        long end = System.currentTimeMillis() - start;
        System.out.println("records：100K，aggregation time：" + TimeUnit.MILLISECONDS.toSeconds(end) + "s");
        List<Row> result = sqlDF.javaRDD().collect();
        int total = 0;
        for (Row row : result) {
            total += row.toString().getBytes().length;
        }
        System.out.println((double) total / (1024 * 1024));
    }

}
