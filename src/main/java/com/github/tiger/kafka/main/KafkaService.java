package com.github.tiger.kafka.main;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.github.tiger.kafka.common.URL;
import com.github.tiger.kafka.consumer.ConsumerMain;
import com.github.tiger.kafka.core.BizEntity;
import com.github.tiger.kafka.core.RemoteConfigure;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author liuhongming
 */
public class KafkaService extends RemoteConfigure {

    /**
     * 主题历史版本
     */
    private Map<String, Integer> versions = Maps.newConcurrentMap();

    private boolean isStartup = false;

    private boolean isRestart = false;

    public KafkaService() {
    }

    @Override
    public void wakeup() {
        reload();
    }

    private void reload() {
        stop(invalidTopic());
        startup(validTopics());
    }

    public void startup(Map<String, BizEntity> validTopics) {
        validTopics.forEach((bizName, bizEntity) -> {
            isRestart = false;

            String topic = bizEntity.getTopic();

            int ver2 = bizEntity.getVersion();
            Integer ver1 = versions.get(topic);

            /**
             * 远程版本大于本地版本时
             * 停止主题消费，变更后重新启动消费
             */
            if (ver1 != null && ver1 < ver2) {
                List<String> topicList = Lists.newArrayList(topic);
                stop(topicList);
                isRestart = true;
            }

            /**
             * 判断未启动或者重新启动时，执行启动操作
             * （重构时将采用位运算实现）
             */
            if (!isStartup || isRestart) {
                startup(bizEntity);
            }

            /**
             * 变更完成，更新版本
             */
            versions.put(topic, ver2);
        });
        isStartup = true;
    }

    private void startup(BizEntity bizEntity) {
        String bizName = bizEntity.getBiz();

        String topic = bizEntity.getTopic();
        List<String> topicList = Lists.newArrayList(topic);

        int nConsumer = Integer.valueOf(bizEntity.getNconsumer());

        String backendUrl = bizEntity.getBackendurl();
        String backendParams = bizEntity.getBackendparams();

        Splitter.MapSplitter mapSplitter = Splitter.on("&").withKeyValueSeparator("=");
        Map<String, String> parameters = mapSplitter.split(backendParams);

        URL dispatchUrl = new URL(backendUrl, new HashMap<>(parameters));
        ConsumerMain.doReceive(nConsumer, bizName, topicList, dispatchUrl);
    }

    public void stop(Collection<String> invalidTopic) {
        invalidTopic.forEach(topic -> ConsumerMain.doCancel(topic));
        /**
         * 清空已取消的主题列表
         */
        invalidTopic.clear();
    }
}
