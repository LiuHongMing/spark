package com.github.tiger.kafka.listener;

import com.github.tiger.kafka.common.URL;
import com.github.tiger.kafka.registry.ZookeeperRegistry;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author liuhongming
 */
public class ZkWatcher implements NodeCacheListener,
        PathChildrenCacheListener {

    private static final Logger logger = LoggerFactory.getLogger(ZkWatcher.class);

    private CuratorFramework client;
    private URL url;
    private NotifyListener listener;

    public ZkWatcher(ZookeeperRegistry registry) {
        this.client = registry.getClient();
        this.url = registry.getURL();
        this.listener = registry.getListener();
    }

    @Override
    public void nodeChanged() throws Exception {
        String path = url.getPath();
        byte[] data = client.getData().forPath(path);
        doNodeChanged(path, data);
        logger.info("Path: {} node changed", path);
    }

    @Override
    public void childEvent(CuratorFramework client, PathChildrenCacheEvent event)
            throws Exception {
        String path = event.getData().getPath();
        byte[] data = event.getData().getData();
        String text = "";
        if (data != null) {
            text = new String(data);
        }
        switch (event.getType()) {
            case CHILD_ADDED:
                logger.info("Path: {} child added, data: {}", path, text);
                break;
            case CHILD_REMOVED:
                logger.info("Path: {} child removed, data: {}", path, text);
                break;
            case CHILD_UPDATED:
                logger.info("Path: {} child updated, data: {}", path, text);
                break;
        }
    }

    private void doNodeChanged(String path, byte[] data) {
        if (data != null) {
            listener.notifyNodeChanged(path, data);
        }
    }
}
