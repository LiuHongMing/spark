package com.github.tiger.kafka.producer;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

/**
 * @author liuhongming
 */
public class ProducerMain {

    private static final Logger logger = LoggerFactory.getLogger(ProducerMain.class);

    public static void doSend(String topic, String key, String value) {
        doSend(topic, null, key, value);
    }

    public static void doSend(String topic, Integer partition, String key, String value) {
        doSend(Arrays.asList(new ProducerRecord(topic, partition, key, value)));
    }

    public static void doSend(List<ProducerRecord> batchList) {
        batchList.forEach(record -> ProducerUtil.async(record.topic(),
                record.partition(), (String) record.key(), (String) record.value()));
    }

}
