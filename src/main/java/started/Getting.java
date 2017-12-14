package started;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

/**
 * @author liuhongming
 */
public class Getting {

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("Getting spark")
                .setMaster("local[*]");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);
        JavaRDD<Integer> distData = jsc.parallelize(data, 2);
        Integer result = distData.reduce((a, b) -> a + b);
        System.out.println(result);
    }

}
