package com.github.tiger.kafka.workbench.test;

import com.github.tiger.kafka.consumer.ConsumerClient;
import com.google.common.collect.Lists;
import kafka.common.KafkaException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Properties;

/**
 * @author liuhongming
 */
public class ConsumerTest {

    private static final Logger logger = LoggerFactory.getLogger(ConsumerClient.class);

    Properties props = new Properties();

    @Before
    public void setUp() {
        props.put("bootstrap.servers", "kafka1:9092,kafka2:9092,kafka3:9092");
        /**
         * 心跳检测的超时时间
         */
        props.put("session.timeout.ms", "10000");
        /**
         * 心跳发送的时间间隔
         */
        props.put("heartbeat.interval.ms", "3000");
        props.put("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        /**
         * 自动偏移量设置
         *
         * earliest:
         * 当各分区下有已提交的offset时，从提交的offset开始消费；
         * 无提交的offset时，从头开始消费
         *
         * latest:
         * 当各分区下有已提交的offset时，从提交的offset开始消费；
         * 无提交的offset时，消费新产生的该分区下的数据
         *
         * none:
         * topic各分区都存在已提交的offset时，从offset后开始消费；
         * 只要有一个分区不存在已提交的offset，则抛出异常
         */
        props.put("auto.offset.reset", "latest");
        /**
         * 关闭自动提交偏移量
         */
        props.put("enable.auto.commit", "false");
        /**
         *
         */
        props.put("max.poll.interval.ms", "300000");
        /**
         *
         */
        props.put("max.poll.records", "100");
    }

    @Test
    public void testReceive() throws InterruptedException {
        List<String> topicList = Lists.newArrayList("benchmark-1-1");
        ConsumerClient client1 = new ConsumerClient();
        client1.receive("consumer-666", topicList);

        ConsumerClient client2 = new ConsumerClient();
        client2.receive("consumer-888", topicList);

        Thread.sleep(1000);

        while (true) {
            synchronized (ConsumerTest.class) {
                try {
                    ConsumerTest.class.wait();
                } catch (InterruptedException e) {
                    wait(1000);

                    throw e;
                }
            }
        }
    }

    /**
     * 手动指定：
     * partition
     * 通过seek自定义轮询offset
     */
    @Test
    public void testAssign() {
        String topic = "benchmark-1-1";

        props.put("group.id", "consumer-1000");

        KafkaConsumer consumer = new KafkaConsumer<>(props);

        TopicPartition tp1 = new TopicPartition(topic, 3);
        /**
         * 手动分配主题分区列表给消费端
         */
        consumer.assign(Arrays.asList(tp1));

        /**
         * 覆盖消费者在下次 poll(timeout) 时使用的提取偏移量。
         * 如果多次为同一分区调用此API，则将在下一个 poll() 中使用最新的偏移量。
         * 请注意，如果此API在使用过程中被任意使用，您可能会丢失数据来重置提取偏移量
         */
        consumer.seek(tp1, 1);

        while (true) {
            /**
             * poll(timeout) 获取使用其中一个订阅/分配API指定的主题或分区的数据。
             *               在轮询数据之前未订阅任何主题或分区是错误的。
             *
             * timeout 如果数据在缓冲区中不可用，则花费在轮询中的时间（以毫秒为单位）。
             *         如果为0，则立即返回缓冲区中当前可用的任何记录，否则返回空。
             */

            long failedOnOffset = 1;
            try {
                ConsumerRecords<String, String> records = consumer.poll(100);
                for (ConsumerRecord<String, String> record : records) {
                    if (record.offset() % 3 == 0) {
                        failedOnOffset = record.offset();
                        logger.info("Error record: {}", record);
                        throw new KafkaException("");
                    } else {
                        logger.info("Consumer received record: {}", record);
                    }
                }
            } catch (Exception e) {
                consumer.seek(tp1, failedOnOffset + 2);
            } finally {
                consumer.close();
            }

            consumer.commitSync();
        }
    }

    /**
     * 自动指定：
     * partition
     */
    @Test
    public void testSubscribe() {
        props.put("group.id", "zpcampus");

        KafkaConsumer consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList("zpcampus2"));

        TopicPartition someTp = new TopicPartition("zpcampus2", 2);

        /**
         * 获取给定分区的*最后提交*的偏移量（理解这句话很关键，无论提交是由这个进程还是另一个进程发生的）。
         * 这个抵消将被用作消费者在发生故障时的位置。
         */
        OffsetAndMetadata offsetAndMetadata = consumer.committed(someTp);
        if (Objects.nonNull(offsetAndMetadata)) {
            String metadata = offsetAndMetadata.toString();
            logger.info("Consumer committed OffsetAndMetadata: {}",
                    metadata);
        }

        while (true) {
            /**
             * poll(timeout) 获取使用其中一个订阅/分配API指定的主题或分区的数据。
             *               在轮询数据之前未订阅任何主题或分区是错误的。
             *
             * timeout 如果数据在缓冲区中不可用，则花费在轮询中的时间（以毫秒为单位）。
             *         如果为0，则立即返回缓冲区中当前可用的任何记录，否则返回空。
             */
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                logger.info("Consumer received record: {}", record);
            }
            consumer.commitSync();
        }
    }

    @Test
    public void testCommitted() {
        props.put("group.id", "zpcampus");

        KafkaConsumer consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList("zpcampus2"));

        TopicPartition someTp = new TopicPartition("zpcampus2", 2);

        /**
         * 获取给定分区的*最后提交*的偏移量（理解这句话很关键，无论提交是由这个进程还是另一个进程发生的）。
         * 这个抵消将被用作消费者在发生故障时的位置。
         */
        OffsetAndMetadata offsetAndMetadata = consumer.committed(someTp);
        if (Objects.nonNull(offsetAndMetadata)) {
            String metadata = offsetAndMetadata.toString();
            logger.info("Consumer committed OffsetAndMetadata: {}",
                    metadata);
        }

        while (true) {
            logger.info("");
        }
    }
}
