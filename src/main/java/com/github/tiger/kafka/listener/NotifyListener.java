package com.github.tiger.kafka.listener;

/**
 * @author liuhongming
 */
public interface NotifyListener {

    /**
     * 有节点数据发生改变时，进行通知
     *
     * @param path
     * @param data
     */
    void notifyNodeChanged(String path, byte[] data);

}
