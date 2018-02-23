package com.github.tiger.kafka.workbench.test;

import com.github.tiger.kafka.core.BizEntity;
import com.github.tiger.kafka.core.RemoteConfigure;
import com.github.tiger.kafka.utils.GsonUtil;
import org.junit.Test;

import java.util.Collection;
import java.util.Map;

/**
 * @author liuhongming
 */
public class ConfigureTest {

    @Test
    public void testRemote() {
        String json = "{\n" +
                "    \"1001\": {\n" +
                "        \"biz\": \"biz1_campus_rd_logs\", \n" +
                "        \"topic\": \"topic1_campus_rd_logs\", \n" +
                "        \"medium\": \"kafka\", \n" +
                "        \"coordinator\": \"host:port\", \n" +
                "        \"nconsumer\": 3, \n" +
                "        \"backendurl\": \"http://campusrdiapi.zhaopin.com/api/operationlog\", \n" +
                "        \"backendparams\": \"source=mqservice&data={msgbody}\", \n" +
                "        \"version\": 2\n" +
                "    }, \n" +
                "    \"1002\": {\n" +
                "        \"biz\": \"biz2_campus_rd_logs\", \n" +
                "        \"topic\": \"topic2_campus_rd_logs\", \n" +
                "        \"medium\": \"kafka\", \n" +
                "        \"coordinator\": \"host:port\", \n" +
                "        \"nconsumer\": 3, \n" +
                "        \"backendurl\": \"http://campusrdiapi.zhaopin.com/api/operationlog\", \n" +
                "        \"backendparams\": \"source=mqservice&data={msgbody}\", \n" +
                "        \"version\": 2\n" +
                "    }, \n" +
                "    \"startup\": [\n" +
                "        \"1001\"\n" +
                "    ], \n" +
                "    \"stop\": [\n" +
                "        \"1002\"\n" +
                "    ]\n" +
                "}";

        System.out.println(json);

        Map config = GsonUtil.fromJson(json, Map.class);
        RemoteConfigure remoteConfigure = new RemoteConfigure();
        remoteConfigure.configure(config);

        Map<String, BizEntity> validTopics = remoteConfigure.validTopics();
        System.out.println(validTopics);

        Collection<String> invalidTopics = remoteConfigure.invalidTopic();
        System.out.println(invalidTopics);
    }

}
