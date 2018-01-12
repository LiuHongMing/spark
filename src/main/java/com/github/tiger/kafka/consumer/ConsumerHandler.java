package com.github.tiger.kafka.consumer;

import com.github.tiger.kafka.core.RecordsFuture;

/**
 * 描述：Kafka消息的消费操作
 * <p>
 * 功能：涵盖消费Kafka消息时的偏移提交，设置操作
 *
 * @author liuhongming
 */
public interface ConsumerHandler<T> extends RecordsFuture<T> {

    /**
     * 处理消息内容
     */
    void handle(T target) throws Exception;

}
