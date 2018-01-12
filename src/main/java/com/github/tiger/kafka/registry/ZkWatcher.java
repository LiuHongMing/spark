package com.github.tiger.kafka.registry;

import com.github.tiger.kafka.common.URL;
import com.github.tiger.kafka.listener.NotifyListener;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
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
        if (event.getData() != null) {
            ChildData childData = event.getData();
            String path = childData.getPath();
            byte[] data = childData.getData();
            switch (event.getType()) {
                case CHILD_ADDED:
                    doChildAdded(path, data);
                    break;
                case CHILD_UPDATED:
                    doChildUpdated(path, data);
                    break;
                case CHILD_REMOVED:
                    doChildRemoved(path, data);
                    break;
            }
        }
    }

    private void doNodeChanged(String path, byte[] data) {
        if (data != null) {
            listener.notifyNodeChanged(path, data);
        }
    }

    private void doChildAdded(String path, byte[] data) {
        String text = "";
        if (data != null) {
            text = new String(data);
        }
        logger.info("Path: {} child added, data: {}", path, text);
    }

    private void doChildUpdated(String path, byte[] data) {
        String text = "";
        if (data != null) {
            text = new String(data);
        }
        logger.info("Path: {} child updated, data: {}", path, text);
    }

    private void doChildRemoved(String path, byte[] data) {
        String text = "";
        if (data != null) {
            text = new String(data);
        }
        logger.info("Path: {} child removed, data: {}", path, text);
    }
}
