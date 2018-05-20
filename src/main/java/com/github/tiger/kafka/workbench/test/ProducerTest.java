package com.github.tiger.kafka.workbench.test;

import com.github.tiger.kafka.producer.ProducerMain;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * @author liuhongming
 */
public class ProducerTest {

    private static final Logger logger = LoggerFactory.getLogger(ProducerTest.class);

    Properties props = new Properties();

    @Before
    public void setUp() {
        props.put("bootstrap.servers", "kafka1:9092,kafka2:9092,kafka3:9092");
        props.put("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        /**
         * leader确认接收消息时的应答方式
         */
        props.put("acks", "1");
        /**
         * 重试次数
         */
        props.put("retries", "0");
        /**
         * 发送时缓冲内存大小
         */
        props.put("buffer.memory", "33554432");
        /**
         * 单个批次的数据大小
         */
        props.put("batch.size", "16384");
        /**
         * 单个批次请求的延迟时间
         */
        props.put("linger.ms", "1");
    }

    @Test
    public void testSend() {
        String topic = "benchmark-1-1";
        int num = 10;
        for (int i = 0; i < num; i++) {
            ProducerMain.doSend(topic, 3, "hello", "world");
        }
        while (true) {
            synchronized (ProducerTest.class) {
                try {
                    ProducerTest.class.wait();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

}
