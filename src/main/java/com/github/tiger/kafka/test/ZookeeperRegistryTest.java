package com.github.tiger.kafka.test;

import com.github.tiger.kafka.registry.ZookeeperRegistry;

/**
 * @author liuhongming
 */
public class ZookeeperRegistryTest {

    public static void main(String[] args) throws Exception {
        ZookeeperRegistry registry = new ZookeeperRegistry("rc", "", "");

        String config = "{\n" +
                "  \"BusinessKeys\": {\n" +
                "    \"1001\": \"CampusRdOpLogs\"\n" +
                "  },\n" +
                "  \"StartupBusinessId\": [\n" +
                "    \"1001\"\n" +
                "  ],\n" +
                "  \"CampusRdOpLogs.Exchang.Name\": \"operatelogs.topic\",\n" +
                "  \"CampusRdOpLogs.Forwarding.Url\": \"http://campusrdiapi.zhaopin.com/api/operationlog/ReceiveOperationLog?_appid=rdiapi\",\n" +
                "  \"CampusRdOpLogs.Forwarding.Url.Params\": \"source=mqservice&data={msgbody}\",\n" +
                "}";

        registry.createNode("/prod/campusposition.json", config.getBytes());

        registry.getClient().setData().forPath("/prod/campusposition.json", config.getBytes());
    }

}
