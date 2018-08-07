package com.github.tiger.spark.demo.java;

import com.google.common.collect.Lists;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.List;

/**
 * @author liuhongming
 */
public class WordCount {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("WordCount");

        JavaSparkContext jsc = new JavaSparkContext(conf);

        JavaRDD<String> lines = jsc.textFile(args[0], 1);

        JavaRDD<String> words = lines.flatMap(
                (FlatMapFunction<String, String>) s -> {
                    List words1 = Lists.newArrayList(s.split(" "));
                    return words1.iterator();
                });

        JavaPairRDD<String, Integer> ones = words.mapToPair(
                (PairFunction<String, String, Integer>) s -> new Tuple2<>(s, 1));

        JavaPairRDD<String, Integer> counts = ones.reduceByKey(
                (Function2<Integer, Integer, Integer>) (i1, i2) -> i1 + i2);

        counts.saveAsTextFile(args[1]);

        jsc.stop();

    }

}
