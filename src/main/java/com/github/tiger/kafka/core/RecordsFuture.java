package com.github.tiger.kafka.core;

/**
 * @author liuhongming
 */
public interface RecordsFuture<T> {

    /**
     * 消息的后续处理
     *
     * @param records
     */
    void commit(T records) throws Exception;

    /**
     * 获取完成状态
     *
     * @return
     */
    boolean isDone();

}
