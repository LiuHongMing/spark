package com.github.tiger.kafka.jmx;

import com.google.common.collect.Lists;

/**
 * @author liuhongming
 */
public class JmxMetricMain {

    public static void main(String[] args) throws Exception {
        JmxConnection jmxConn = new JmxConnection("192.168.66.11:9988");
        jmxConn.init();

        while (true) {
            String topicName = "benchmark-1-1";
            /**
             * 与topic无关的metric
             */
            Object o1 = jmxConn.getValue(
                    "kafka.server:type=ReplicaManager,name=PartitionCount",
                    Lists.newArrayList("Value"));
            System.out.println(o1);

            /**
             * 与topic有关的metric
             */
            Object o2 = jmxConn.getValue(topicName,
                    "kafka.server:type=BrokerTopicMetrics,name=BytesInPerSec",
                    Lists.newArrayList("Count", "OneMinuteRate", "FiveMinuteRate"));
            System.out.println(o2);

            Thread.sleep(5000);
        }
    }
}
