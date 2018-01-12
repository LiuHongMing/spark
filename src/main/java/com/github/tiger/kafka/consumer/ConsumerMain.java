package com.github.tiger.kafka.consumer;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.github.tiger.kafka.common.Closeable;
import com.github.tiger.kafka.common.URL;
import com.github.tiger.kafka.consumer.handler.HttpDispatcher;
import org.apache.commons.lang3.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author liuhongming
 */
public class ConsumerMain {

    private static final Logger logger = LoggerFactory.getLogger(ConsumerMain.class);

    private static Map<String, Set<Closeable>> activeConsumer = Maps.newConcurrentMap();

    public static void doReceive(int nConsumer, List<String> topicList, URL dispatchUrl) {
        doReceive(nConsumer, ConsumerClient.DEFAULT_GROUP_ID, topicList, dispatchUrl);
    }

    public static void doReceive(int nConsumer, String groupId,
                                 List<String> topicList, URL dispatchUrl) {
        logger.info("doReceive({}, {}, {}, {})", nConsumer, groupId, topicList,
                dispatchUrl);
        String clientName = RandomStringUtils.randomNumeric(4);
        for (int i = 0; i < nConsumer; i++) {
            HttpDispatcher handler = new HttpDispatcher(dispatchUrl);
            ConsumerClient client = new ConsumerClient(clientName,
                    handler);
            client.receive(groupId, topicList);

            topicList.forEach(topic -> {
                Set<Closeable> activeSet = activeConsumer.get(topic);
                if (activeSet == null) {
                    activeSet = Sets.newHashSet();
                    activeConsumer.put(topic, activeSet);
                }
                activeSet.add(client);
            });
        }
    }

    public static Set<Closeable> getActiveConsumer(String topic) {
        Set<Closeable> consumers = activeConsumer.get(topic);
        return consumers;
    }

    public static void doCancel(String topic) {
        Set<Closeable> consumers = activeConsumer.get(topic);
        if (consumers != null) {
            consumers.forEach(consumer -> {
                if (consumer != null) {
                    consumer.close();
                }
            });
        }
        activeConsumer.remove(topic);
    }

}
