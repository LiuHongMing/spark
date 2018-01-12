package com.github.tiger.kafka.test;

import com.github.tiger.kafka.producer.ProducerMain;
import org.junit.Test;

/**
 * @author liuhongming
 */
public class ProducerTest {

    @Test
    public void testSend() {
        ProducerMain.doSend("zpcampus1", "campus", "hehe");
        ProducerMain.doSend("zpcampus2", "campus", "haha");
    }

}
