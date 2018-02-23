package com.github.tiger.kafka.workbench.test;

import com.github.tiger.kafka.main.KafkaMain;

/**
 * @author liuhongming
 */
public class KafkaMainTest {

    public static void main(String[] args) throws Exception {
        String[] arguments = new String[]{"-conf", "campusrc"};
        KafkaMain.main(arguments);
    }

}
