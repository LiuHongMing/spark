package com.github.tiger.kafka.workbench.test;

import com.github.tiger.kafka.zookeeper.ZookeeperRegistry;
import org.junit.Before;
import org.junit.Test;

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

    @Before
    public void setUp() throws Exception {
        zkRegistry = new ZookeeperRegistry("kafka", null, null);
    }

    @Test
    public void testZkCrud() throws Exception {
        zkRegistry.createNode("/qa/campusrc.json", json.getBytes());
        zkRegistry.getClient().setData().forPath("/qa/campusrc.json", json.getBytes());
    }

}
