package com.github.tiger.kafka.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * @author liuhongming
 */
public class ConsumerMain {

    private static final Logger logger = LoggerFactory.getLogger(ConsumerMain.class);

    public static void doReceive(int nConsumer, List<String> topicList, ConsumerHandler handler) {
        doReceive(nConsumer, ConsumerClient.DEFAULT_GROUP_ID, topicList, handler);
    }

    public static void doReceive(int nConsumer, String groupId,
                                 List<String> topicList, ConsumerHandler handler) {
        logger.info("doReceive({}, {}, {}, {})", nConsumer, groupId, topicList,
                handler.getClass().getCanonicalName());
        for (int i = 0; i < nConsumer; i++) {
            ConsumerClient client = new ConsumerClient(ConsumerClient.DEFAULT_CLIENT_NAME,
                    handler);
            client.receive(groupId, topicList);
        }
    }
}
