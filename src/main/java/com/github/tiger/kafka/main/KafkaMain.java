package com.github.tiger.kafka.main;

import com.google.common.base.Joiner;
import com.github.tiger.kafka.common.URL;
import com.github.tiger.kafka.config.Constants;
import com.github.tiger.kafka.registry.Registry;
import com.github.tiger.kafka.zookeeper.ZookeeperRegistry;
import com.github.tiger.kafka.utils.PropertiesUtil;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;

import java.io.File;
import java.util.Properties;

/**
 * @author liuhongming
 */
public class KafkaMain {

    private static Properties zkProps;

    static {
        zkProps = PropertiesUtil.load("zookeeper.properties",
                KafkaMain.class.getClassLoader());
    }

    public static void main(String[] args) throws Exception {
        String conf;

        String requiredName = "conf";

        Options options = new Options();
        options.addOption(requiredName, true, "A configuration node name");

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(options, args);
        if (!cmd.hasOption(requiredName)) {
            throw new Exception("Need to configure the node name");
        } else {
            conf = cmd.getOptionValue(requiredName);
            if (conf == null) {
                throw new Exception("Configuration node name is null");
            }
        }

        String namespace = getZookeeperProperty(Constants.ZOOKEEPER_NAMESPACE);
        String environment = getZookeeperProperty(Constants.ZOOKEEPER_ENVIRONMENT);
        String site = getZookeeperProperty(Constants.ZOOKEEPER_SITE);

        String postfix = getZookeeperProperty(Constants.CONFIG_POSTFIX);

        String path = Joiner.on(File.separator).join(
                new String[]{"", environment, site, conf}).concat("." + postfix);

        Registry registry = new ZookeeperRegistry(namespace, null, null);
        URL subscribeUrl = new URL(path);
        KafkaService service = new KafkaService();
        service.registry(registry).sync(subscribeUrl);

        while (true) {
            synchronized (KafkaMain.class) {
                try {
                    KafkaMain.class.wait();
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
