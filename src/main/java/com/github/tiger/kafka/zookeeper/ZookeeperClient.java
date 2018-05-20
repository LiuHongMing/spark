package com.github.tiger.kafka.zookeeper;

import com.github.tiger.kafka.common.URL;

import java.util.List;

public interface ZookeeperClient {

    void create(String path, boolean ephemeral);

    void delete(String path);

    List<String> getChildren(String path);

    boolean isConnected();

    void close();

    URL getUrl();

}
