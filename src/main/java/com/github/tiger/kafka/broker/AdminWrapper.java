package com.github.tiger.kafka.broker;

import com.github.tiger.kafka.utils.PropertiesUtil;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.admin.*;

import java.util.*;
import java.util.concurrent.ExecutionException;

/**
 * @author liuhongming
 */
public class AdminWrapper {

    protected static Properties configs;

    static {
        String conf = "conf/broker.yml";
        configs = PropertiesUtil.loadYaml(conf);
    }

    public static void createTopics(AdminClient client, String[] topics, int numPartitions,
                                    short replicationFactor) throws ExecutionException,
            InterruptedException {
        List<NewTopic> newTopics = Lists.newArrayList();
        if (Objects.nonNull(topics)) {
            for (String topic : topics) {
                newTopics.add(new NewTopic(topic,
                        numPartitions, replicationFactor));
            }
        }
        CreateTopicsResult createResult = client.createTopics(newTopics);
        createResult.all().get();
    }

    public static void deleteTopics(AdminClient client, String[] topics) throws ExecutionException,
            InterruptedException {
        List<String> deleteTopics = Arrays.asList(topics);
        DeleteTopicsResult deleteResult = client.deleteTopics(deleteTopics);
        deleteResult.all().get();
    }

    public static Set<String> listTopics(AdminClient client) throws ExecutionException,
            InterruptedException {
        ListTopicsResult topicsResult = client.listTopics();
        Set<String> topicNames = topicsResult.names().get();
        return topicNames;
    }

    public static AdminClient getClient() {
        return getClient(configs.getProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG));
    }

    public static AdminClient getClient(String bootstrapServers) {
        return getClient(bootstrapServers, new Properties());
    }

    public static AdminClient getClient(String bootstrapServers, Properties otherProperties) {
        Properties conf = new Properties();
        if (StringUtils.isNotEmpty(bootstrapServers)) {
            conf.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,
                    bootstrapServers);
        }
        if (Objects.nonNull(otherProperties)) {
            conf.putAll(otherProperties);
        }
        return KafkaAdminClient.create(conf);
    }

}
