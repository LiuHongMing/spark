package com.github.tiger.kafka.config;

import com.google.common.collect.Maps;
import com.github.tiger.kafka.utils.GsonUtil;
import com.github.tiger.kafka.common.URL;
import com.github.tiger.kafka.listener.NotifyListener;
import com.github.tiger.kafka.registry.Registry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;

/**
 * @author liuhongming
 */
public abstract class Configurable implements NotifyListener {

    public static final Logger logger = LoggerFactory.getLogger(Configurable.class);

    private static final Map<String, Object> ALL = Maps.newConcurrentMap();

    private Registry registry;

    public Configurable() {
    }

    public Configurable(Registry registry) throws RegistryException {
        init(registry);
    }

    private void init(Registry registry) throws RegistryException {
        this.registry = registry;
        if (this.registry == null) {
            throw new RegistryException("Registry can not be null");
        }
        this.registry.addListener(this);
    }

    public Configurable registry(Registry registry) throws RegistryException {
        init(registry);
        return this;
    }

    /**
     * 订阅注册信息
     *
     * @param url 注册信息路径
     *
     * @throws RegistryException
     */
    public void sync(URL url) throws RegistryException {
        if (url == null) {
            throw new RegistryException("Subscribe url can not be null");
        }
        registry.subscribe(url);
    }

    /**
     * 使用键值对进行配置
     *
     * @param configs
     */
    public abstract void configure(Map<String, ?> configs);

    /**
     * 更新配置
     *
     * @param configs
     */
    protected void update(Map<String, ?> configs) {
        ALL.putAll(configs);
        configure(configs);
    }

    @Override
    public void notifyNodeChanged(String path, byte[] data) {
        Map<String, Object> configMap = Maps.newHashMap();

        String content = new String(data);
        boolean isProperties = path.endsWith(".properties");
        if (isProperties) {
            configMap.putAll((Map) GsonUtil.fromJson(content, Properties.class));
        } else {
            configMap.putAll(GsonUtil.fromJson(content, Map.class));
        }

        update(configMap);
    }

    public class RegistryException extends Exception {

        public RegistryException(String message) {
            super(message);
        }

        public RegistryException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
