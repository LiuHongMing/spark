package com.github.tiger.kafka.registry;

import com.github.tiger.kafka.common.Node;
import com.github.tiger.kafka.common.URL;
import com.github.tiger.kafka.listener.NotifyListener;

/**
 * @author liuhongming
 */
public interface Registry extends Node {

    /**
     * 订阅发布
     *
     * @param url
     */
    void subscribe(URL url);

    /**
     * 添加监听器
     *
     * @param listener
     */
    void addListener(NotifyListener listener);
}
