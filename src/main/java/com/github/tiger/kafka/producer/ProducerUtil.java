package com.github.tiger.kafka.producer;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * 描述：消息生产者工具类
 * <p>
 * 功能：用于发送消息至Kafka
 *
 * @author liuhongming
 */
public class ProducerUtil {

    private static final Logger logger = LoggerFactory.getLogger(ProducerUtil.class);

    private static final Producer<String, String> producer = ProducerFactory.getInstance();

    public static void sync(String topic, Integer partition, String key, String value) {
        send(topic, partition, key, value, false);
    }

    public static void async(String topic, Integer partition, String key, String value) {
        send(topic, partition, key, value, true);
    }

    public static void send(String topic, Integer partition,
                            String key, String value, boolean isAsync) {
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, partition, key, value);
        if (isAsync) {
            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    logger.error("An exception occurred while the producer send record", exception);
                }
                logger.info("Producer sent record: topic={}, partition={}, offset={}",
                        metadata.topic(), metadata.partition(), metadata.offset());
            });
        } else {
            try {
                producer.send(record).get();
            } catch (InterruptedException | ExecutionException ex) {
                logger.error("An exception occurred while the producer send record", ex);
            }
        }
    }

    public static void close() {
        producer.close();
    }
}