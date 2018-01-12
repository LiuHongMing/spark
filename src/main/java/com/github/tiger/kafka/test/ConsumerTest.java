package com.github.tiger.kafka.test;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.github.tiger.kafka.common.URL;
import com.github.tiger.kafka.consumer.ConsumerMain;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @author liuhongming
 */
public class ConsumerTest {

    @Test
    public void testReceive() throws InterruptedException {
        List<String> topicList = Lists.newArrayList("zpcampus1", "zpcampus2");
        URL url = new URL("http://www.58.com", Maps.newHashMap());
        ConsumerMain.doReceive(2, topicList, url);

        TimeUnit.SECONDS.sleep(30);

        ConsumerMain.doCancel("zpcampus1");

        while (true) {
            synchronized (ConsumerTest.class) {
                try {
                    ConsumerTest.class.wait();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

}
