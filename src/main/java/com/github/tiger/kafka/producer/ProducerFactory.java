package com.github.tiger.kafka.producer;

import com.github.tiger.kafka.utils.PropertiesUtil;
import com.google.common.collect.Maps;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

/**
 * 描述：KafkaProducer工厂类
 * <p>
 * 功能：创建KafkaProducer实例
 *
 * @author liuhongming
 */
public class ProducerFactory {

    private static Properties configs;

    static {
        String yaml = "conf/kafka/producer.yaml";
        configs = PropertiesUtil.loadYaml(yaml);
    }


    public static Producer<String, String> getInstance() {
        return getInstance(configs);
    }

    public static Producer<String, String> getInstance(Properties props) {
        HashMap<String, Object> configs = Maps.newHashMap();
        if (Objects.nonNull(props)) {
            configs.putAll((Map) props);
        }
        return new KafkaProducer(configs);
    }
}