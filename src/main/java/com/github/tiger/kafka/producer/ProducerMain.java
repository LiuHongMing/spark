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
        doSend(Arrays.asList(new ProducerRecord(topic, key, value)));
    }

    public static void doSend(List<ProducerRecord> batchList) {
        batchList.forEach(record -> ProducerUtil.send(record.topic(),
                (String) record.key(), (String) record.value()));
    }

}
