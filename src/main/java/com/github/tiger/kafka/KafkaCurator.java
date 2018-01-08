package com.github.tiger.kafka;

import com.github.tiger.kafka.common.Constants;
import com.github.tiger.kafka.common.URL;
import com.github.tiger.kafka.consumer.ConsumerMain;
import com.github.tiger.kafka.consumer.handler.HttpDispatcher;
import com.github.tiger.kafka.registry.Registry;
import com.github.tiger.kafka.registry.ZookeeperRegistry;
import com.github.tiger.kafka.utils.PropertiesUtil;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.io.File;
import java.util.List;
import java.util.Properties;

/**
 * @author liuhongming
 */
public class KafkaCurator {

    private static Properties zkProps;

    static {
        zkProps = PropertiesUtil.load("conf/zookeeper_conf.properties",
                KafkaCurator.class.getClassLoader());
    }

    public static void main(String[] args) throws Exception {

        String namespace = getZookeeperProperty(Constants.ZOOKEEPER_NAMESPACE);
        String environment = getZookeeperProperty(Constants.ZOOKEEPER_ENVIRONMENT);
        String site = getZookeeperProperty(Constants.ZOOKEEPER_SITE);
        String postfix = getZookeeperProperty(Constants.CONFIG_POSTFIX);

        String path = Joiner.on(File.separator).join(
                new String[]{"", environment, site}).concat("." + postfix);

        Configuration configuration = new Configuration(new ZookeeperRegistry(
                namespace, "", ""));
        configuration.sync(new URL(path));

        while (true) {
            synchronized (KafkaCurator.class) {
                try {
                    KafkaCurator.class.wait();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static String getZookeeperProperty(String key) {
        return zkProps.getProperty(key);
    }

    public static int getZookeeperProperty2Int(String key) {
        return Integer.valueOf(zkProps.getProperty(key, "0"));
    }

}
