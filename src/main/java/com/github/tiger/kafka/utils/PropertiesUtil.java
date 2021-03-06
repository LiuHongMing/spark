package com.github.tiger.kafka.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.YamlPropertiesFactoryBean;
import org.springframework.core.io.ClassPathResource;

import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;
import java.util.Properties;

/**
 * @author liuhongming
 */
public class PropertiesUtil {

    private static final Logger logger = LoggerFactory.getLogger(PropertiesUtil.class);

    private PropertiesUtil() {}

    public static Properties load(String resource, ClassLoader classLoader) {
        if (Objects.isNull(classLoader)) {
            classLoader = Thread.currentThread().getContextClassLoader();
        }
        InputStream in = classLoader.getResourceAsStream(resource);
        Properties props = new Properties();
        try {
            props.load(in);
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }
        return props;
    }

    public static Properties loadYaml(String resource) {
        YamlPropertiesFactoryBean yaml = new YamlPropertiesFactoryBean();
        yaml.setResources(new ClassPathResource(resource));
        return yaml.getObject();
    }

}
