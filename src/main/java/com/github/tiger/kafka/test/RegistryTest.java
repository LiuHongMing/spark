package com.github.tiger.kafka.test;

import com.github.tiger.kafka.utils.GsonUtil;
import com.github.tiger.kafka.core.MqConfigure;
import com.github.tiger.kafka.registry.ZookeeperRegistry;
import org.junit.Test;

import java.util.Map;

/**
 * @author liuhongming
 */
public class RegistryTest {

    ZookeeperRegistry zkRegistry = new ZookeeperRegistry("kafka", null, null);

    String json = "{\n" +
            "    \"1001\": {\n" +
            "        \"biz\": \"biz1\", \n" +
            "        \"topic\": \"zpcampus1\", \n" +
            "        \"medium\": \"kafka\", \n" +
            "        \"coordinator\": \"host:port\", \n" +
            "        \"nconsumer\": 3, \n" +
            "        \"backendurl\": \"http://campusrdiapi.zhaopin.com/api/operationlog\", \n" +
            "        \"backendparams\": \"source=mqservice&data={msgbody}\", \n" +
            "        \"version\": 2\n" +
            "    }, \n" +
            "    \"1002\": {\n" +
            "        \"biz\": \"biz2\", \n" +
            "        \"topic\": \"zpcampus2\", \n" +
            "        \"medium\": \"kafka\", \n" +
            "        \"coordinator\": \"host:port\", \n" +
            "        \"nconsumer\": 3, \n" +
            "        \"backendurl\": \"http://campusrdiapi.zhaopin.com/api/operationlog\", \n" +
            "        \"backendparams\": \"source=mqservice&data={msgbody}\", \n" +
            "        \"version\": 9\n" +
            "    }, \n" +
            "    \"startup\": [\n" +
            "        \"1001\",\n" +
            "        \"1002\"\n" +
            "    ], \n" +
            "    \"stop\": [\n" +
            "    ]\n" +
            "}";

    @Test
    public void testZkCrud() throws Exception {
        zkRegistry.createNode("/qa/campusrc.json", json.getBytes());
        zkRegistry.getClient().setData().forPath("/qa/campusrc.json", json.getBytes());
    }

    @Test
    public void testMqConfigure() throws Exception {
        MqConfigure configure = new MqConfigure();
        Map<String, ?> map = GsonUtil.fromJson(json, Map.class);
        configure.configure(map);
    }

}
