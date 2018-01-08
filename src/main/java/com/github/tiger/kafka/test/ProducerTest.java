package com.github.tiger.kafka.test;

import com.github.tiger.kafka.producer.ProducerMain;

/**
 * @author liuhongming
 */
public class ProducerTest {

    public static void main(String[] args) {
        ProducerMain.doSend("zpcampus1", "campus", "hehe");
        ProducerMain.doSend("zpcampus2", "campus", "haha");
    }

}
