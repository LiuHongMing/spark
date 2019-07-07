package com.github.tiger.spark.streaming.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.regex.Pattern;

/**
 * @author liuhongming
 */
public class JavaKafkaWordCount {

    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) throws InterruptedException {

        SparkConf sparkConf = new SparkConf()
                .setMaster("local[*]")
                .setAppName("JavaKafkaWordCount");

        // Create the context with 2 seconds batch size
        JavaStreamingContext jssc =
                new JavaStreamingContext(sparkConf, new Duration(2000));

        List<String> topics = Arrays.asList("streaming-spark-2");
        HashMap<String, Object> kafkaParams = new HashMap(8);
        kafkaParams.put("bootstrap.servers",
                "kafka126:9092,kafka128:9092,kafka129:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "streaming-group-2");
        kafkaParams.put("auto.offset.reset", "earliest");
        kafkaParams.put("enable.auto.commit", "false");

        JavaInputDStream<ConsumerRecord<String, String>> messages =
                KafkaUtils.createDirectStream(jssc,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams));

        JavaDStream<String> lines = messages.map(
                (Function<ConsumerRecord<String, String>, String>) record -> record.value());

        JavaDStream<String> words = lines.flatMap(
                (FlatMapFunction<String, String>) x -> Arrays.asList(SPACE.split(x)).iterator());

        JavaPairDStream<String, Integer> wordCounts = words.mapToPair(
                (PairFunction<String, String, Integer>) s -> new Tuple2<>(s, 1))
                .reduceByKey((Function2<Integer, Integer, Integer>) (i1, i2) -> i1 + i2);

        wordCounts.print(100);

        jssc.start();
        jssc.awaitTermination();
    }

}
