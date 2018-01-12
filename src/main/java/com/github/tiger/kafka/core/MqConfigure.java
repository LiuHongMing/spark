package com.github.tiger.kafka.core;

import com.google.common.collect.Maps;
import com.github.tiger.kafka.config.Configurable;
import com.github.tiger.kafka.registry.Registry;
import org.apache.commons.beanutils.BeanUtils;

import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.Map;

/**
 * @author liuhongming
 */
public class MqConfigure extends Configurable {

    private Map<String, BizEntity> validTopics = Maps.newHashMap();

    private Map<String, BizEntity> invalidTopics = Maps.newHashMap();

    private Map<String, ?> configs = Maps.newHashMap();

    public MqConfigure() {
    }

    public MqConfigure(Registry registry) throws RegistryException {
        super(registry);
    }

    @Override
    public void configure(Map<String, ?> configs) {
        this.configs = configs;
        compareAndWrap();
        wakeup();
    }

    public void wakeup() {}

    public Map<String, BizEntity> validTopics() {
        return validTopics;
    }

    public Collection<String> invalidTopic() {
        return invalidTopics.keySet();
    }

    private void compareAndWrap() {
        Object validIds = configs.get(Constants.STARTUP_INDEX);
        if (validIds instanceof Collection) {
            ((Collection) validIds).forEach(id -> {
                BizEntity entity = getBizEntity(String.valueOf(id));
                validTopics.put(entity.getTopic(), entity);
            });
        }
        Object invalidIds = configs.get(Constants.STOP_INDEX);
        if (invalidIds instanceof Collection) {
            ((Collection) invalidIds).forEach(id -> {
                BizEntity entity = getBizEntity(String.valueOf(id));
                invalidTopics.put(entity.getTopic(), entity);
            });
        }
    }

    public BizEntity getBizEntity(String id) {
        BizEntity entity = new BizEntity();

        Object element = configs.get(id);
        if (element instanceof Map) {
            try {
                BeanUtils.populate(entity, (Map) element);
            } catch (IllegalAccessException | InvocationTargetException e) {
                e.printStackTrace();
            }
        }

        return entity;
    }

}
