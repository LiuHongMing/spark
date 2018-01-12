package com.github.tiger.kafka.consumer;

import com.google.common.collect.Maps;
import com.github.tiger.kafka.utils.PropertiesUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

/**
 * 描述：KafkaConsumer工厂类
 * <p>
 * 功能：创建KafkaConsumer实例
 *
 * @author liuhongming
 */
public class ConsumerFactory {

    private static Properties configs;

    static {
        String conf = "conf/kafka/consumer.yaml";
        configs = PropertiesUtil.loadYaml(conf);
    }

    public static Consumer<String, String> group(String groupId) {
        return group(groupId, null);
    }

    public static Consumer<String, String> group(String groupId, String clientId) {
        if (StringUtils.isNotEmpty(groupId)) {
            configs.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        }
        if (StringUtils.isNotEmpty(clientId)) {
            configs.put(ConsumerConfig.CLIENT_ID_CONFIG, clientId);
        }
        return getInstance(configs);
    }

    public static Consumer<String, String> getInstance(Properties props) {
        HashMap<String, Object> configs = Maps.newHashMap();
        if (Objects.nonNull(props)) {
            configs.putAll((Map) props);
        }
        return new KafkaConsumer(configs);
    }

}
