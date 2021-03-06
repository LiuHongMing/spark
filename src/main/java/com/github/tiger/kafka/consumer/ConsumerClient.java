package com.github.tiger.kafka.consumer;

import com.google.common.collect.Lists;
import com.github.tiger.kafka.common.Closeable;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 描述：消费者客户端
 * <p>
 * 功能：用于从Kafka获取消息
 *
 * @author liuhongming
 */
public class ConsumerClient implements Closeable {

    private static final Logger logger = LoggerFactory.getLogger(ConsumerClient.class);

    private static ConsumerHandler NO_HANDLE = new ConsumerHandler() {
        @Override
        public void commit(Object records) {
        }

        @Override
        public boolean isDone() {
            return false;
        }

        @Override
        public void handle(Object object) {
        }
    };

    private static final int MIN_BATCH_SIZE = 1;

    private static final int MAX_BATCH_SIZE = Integer.MAX_VALUE;

    private static final AtomicInteger ID = new AtomicInteger(1);

    private static final long POOL_TIMEOUT = 10 * 1000;

    private String clientId;

    private String clientName;

    private int batchSize;

    private volatile boolean isClosed = false;

    private Consumer<String, String> consumer;

    private ConsumerHandler handler;

    public static final String DEFAULT_CLIENT_NAME = "client";

    public static final String DEFAULT_GROUP_ID = "mygroup";

    public ConsumerClient() {
        this(DEFAULT_CLIENT_NAME, MIN_BATCH_SIZE, NO_HANDLE);
    }

    public ConsumerClient(String clientName, ConsumerHandler handler) {
        this(clientName, MIN_BATCH_SIZE, handler);
    }

    public ConsumerClient(String clientName, int batchSize, ConsumerHandler handler) {
        this.clientName = clientName;
        this.batchSize = batchSize;
        this.handler = handler;
    }

    public void receive(String groupId, List<String> topics) {
        if (StringUtils.isEmpty(groupId)) {
            groupId = DEFAULT_GROUP_ID;
        }
        if (StringUtils.isEmpty(clientName)) {
            clientName = DEFAULT_CLIENT_NAME;
        }
        clientId = String.format("%s-consumer-%s-%d", groupId, clientName,
                ID.getAndIncrement());
        consumer = ConsumerFactory.group(groupId, clientId);
        Runner runner = new Runner(groupId, topics);
        runner.start();
    }

    @Override
    public void close() {
        isClosed = true;
        logger.info("Close the consumer({})", clientId);
    }

    public boolean isClosed() {
        return isClosed;
    }

    @Override
    public String toString() {
        return "ConsumerClient{" +
                "clientId='" + clientId + '\'' +
                '}';
    }

    private class Runner implements Runnable {

        private String groupId;

        private List<String> topics;

        public Runner(String groupId, List<String> topics) {
            this.groupId = groupId;
            this.topics = topics;
        }

        public void start() {
            ThreadGroup group = new ThreadGroup(groupId);
            Thread t = new Thread(group, this, clientId, 0);
            t.start();
        }

        @Override
        public void run() {
            List<ConsumerRecord<String, String>> buffer = Lists.newArrayList();
            consumer.subscribe(this.topics);
            while (!isClosed()) {
                /**
                 * poll(timeout) 获取使用其中一个订阅/分配API指定的主题或分区的数据。
                 *               在轮询数据之前未订阅任何主题或分区是错误的。
                 *
                 * timeout 如果数据在缓冲区中不可用，则花费在轮询中的时间（以毫秒为单位）。
                 *         如果为0，则立即返回缓冲区中当前可用的任何记录，否则返回空。
                 */
                ConsumerRecords<String, String> records = consumer.poll(POOL_TIMEOUT);
                for (ConsumerRecord<String, String> record : records) {
                    logger.info("Consumer received record: {}", record);
                    buffer.add(record);
                }
                /**
                 *  数据达到批处理数量时，同步确认offset
                 */
                try {
                    if (buffer.size() >= batchSize) {
                        if (handler != null) {
                            handler.commit(buffer);
                        }
                        consumer.commitSync();
                        buffer.clear();
                    }
                } catch (Exception e) {
                    logger.error("An exception occured while commit records", e);
                }
            }
            consumer.close();
        }
    }


}
