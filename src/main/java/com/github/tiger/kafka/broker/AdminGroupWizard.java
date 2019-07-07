package com.github.tiger.kafka.broker;

import kafka.admin.AdminClient;
import kafka.coordinator.group.GroupOverview;
import org.apache.kafka.common.TopicPartition;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static scala.collection.JavaConversions.mapAsJavaMap;
import static scala.collection.JavaConversions.seqAsJavaList;

/**
 * @author liuhongming
 */
public class AdminGroupWizard extends AdminWrapper {

    public static Set<String> getAllGroupsForTopic(String bootstrapServers, String topic) {

        AdminClient client = AdminClient.createSimplePlaintext(bootstrapServers);

        try {
            java.util.List<GroupOverview> allGroups =
                    seqAsJavaList(client.listAllGroupsFlattened().toSeq());

            Set<String> groups = new HashSet<>();

            for (GroupOverview overview : allGroups) {

                String groupId = overview.groupId();
                Map<TopicPartition, Object> offsets =
                        mapAsJavaMap(client.listGroupOffsets(groupId));

                Set<TopicPartition> partitions = offsets.keySet();
                for (TopicPartition tp : partitions) {
                    if (tp.topic().equals(topic)) {
                        groups.add(groupId);
                    }
                }

            }

            return groups;

        } finally {
            client.close();
        }
    }

    public static void main(String[] args) {
        getAllGroupsForTopic(
                "kafka1:9092,kafka2:9092,kafka3:9092",
                "__consumer_offsets").forEach(s -> System.out.println(s));
    }

}
