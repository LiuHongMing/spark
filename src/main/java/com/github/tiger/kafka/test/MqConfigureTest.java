package com.github.tiger.kafka.test;

import com.github.tiger.gson.GsonUtil;
import com.github.tiger.kafka.core.BizEntity;
import com.github.tiger.kafka.core.MqConfigure;

import java.util.Collection;
import java.util.Map;

/**
 * @author liuhongming
 */
public class MqConfigureTest {

    public static void main(String[] args) {
        String json = "{\n" +
                "    \"1001\": {\n" +
                "        \"bizName\": \"biz1_campus_rd_logs\", \n" +
                "        \"topicName\": \"topic1_campus_rd_logs\", \n" +
                "        \"mqMedium\": \"kafka\", \n" +
                "        \"coordinator\": \"host:port\", \n" +
                "        \"nConsumer\": 3, \n" +
                "        \"backendUrl\": \"http://campusrdiapi.zhaopin.com/api/operationlog\", \n" +
                "        \"backendParams\": \"source=mqservice&data={msgbody}\", \n" +
                "        \"version\": 2\n" +
                "    }, \n" +
                "    \"1002\": {\n" +
                "        \"bizName\": \"biz2_campus_rd_logs\", \n" +
                "        \"topicName\": \"topic2_campus_rd_logs\", \n" +
                "        \"mqMedium\": \"kafka\", \n" +
                "        \"coordinator\": \"host:port\", \n" +
                "        \"nConsumer\": 3, \n" +
                "        \"backendUrl\": \"http://campusrdiapi.zhaopin.com/api/operationlog\", \n" +
                "        \"backendParams\": \"source=mqservice&data={msgbody}\", \n" +
                "        \"version\": 2\n" +
                "    }, \n" +
                "    \"startupIndex\": [\n" +
                "        \"1001\"\n" +
                "    ], \n" +
                "    \"stopIndex\": [\n" +
                "        \"1002\"\n" +
                "    ]\n" +
                "}";

        Map config = GsonUtil.fromJson(json, Map.class);
        MqConfigure mqConfigure = new MqConfigure();
        mqConfigure.configure(config);

        Map<String, BizEntity> validTopics = mqConfigure.validTopics();
        System.out.println(validTopics);

        Collection<String> invalidTopics = mqConfigure.invalidTopic();
        System.out.println(invalidTopics);
    }

}
