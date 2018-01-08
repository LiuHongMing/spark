package com.github.tiger.kafka.test;

import com.github.tiger.kafka.common.URL;
import com.github.tiger.kafka.consumer.ConsumerMain;
import com.github.tiger.kafka.consumer.handler.HttpDispatcher;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.List;

/**
 * @author liuhongming
 */
public class ConsumerTest {

    public static void main(String[] args) {
        List<String> topicList = Lists.newArrayList("zpcampus1", "zpcampus2");
        URL url = new URL("http://www.58.com", Maps.newHashMap());
        HttpDispatcher handler = new HttpDispatcher(url);

        ConsumerMain.doReceive(2, topicList, handler);
    }

}
