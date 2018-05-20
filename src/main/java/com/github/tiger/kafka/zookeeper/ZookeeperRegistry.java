package com.github.tiger.kafka.zookeeper;

import com.github.tiger.kafka.common.URL;
import com.github.tiger.kafka.config.Constants;
import com.github.tiger.kafka.listener.NotifyListener;
import com.github.tiger.kafka.main.KafkaMain;
import com.github.tiger.kafka.registry.Registry;
import org.apache.commons.lang3.StringUtils;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.ACLProvider;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.server.auth.DigestAuthenticationProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author liuhongming
 */
public class ZookeeperRegistry implements Registry {

    private static final Logger logger = LoggerFactory.getLogger(ZookeeperRegistry.class);

    private final static int DEFAULT_ZOOKEEPER_PORT = 2181;

    private static final String DEFAULT_CONNECT_STRING = KafkaMain
            .getZookeeperProperty(Constants.ZOOKEEPER_CONNECT_STRING);
    private static final String DEFAULT_NAMESPACE = KafkaMain
            .getZookeeperProperty(Constants.ZOOKEEPER_NAMESPACE);
    private static final int DEFAULT_CONNECTION_TIMEOUT = KafkaMain
            .getZookeeperProperty2Int(Constants.ZOOKEEPER_CONNECTION_TIMEOUT);
    private static final int DEFAULT_SESSION_TIMEOUT = KafkaMain
            .getZookeeperProperty2Int(Constants.ZOOKEEPER_SESSION_TIMEOUT);
    private static final boolean DEFAULT_READ_ONLY = true;

    private static final String DEFAULT_SCHEMA = "digest";
    private static final String DEFAULT_AUTH = "zookeeper:zhaopin";

    private String namespace;
    private String schema;
    private String auth;

    private URL url;
    private ZookeeperWatcher watcher;
    private NotifyListener listener;
    private CuratorFramework client;

    public ZookeeperRegistry() {
        this(DEFAULT_NAMESPACE, DEFAULT_SCHEMA, DEFAULT_AUTH);
    }

    public ZookeeperRegistry(String namespace) {
        this(namespace, DEFAULT_SCHEMA, DEFAULT_AUTH);
    }

    public ZookeeperRegistry(String namespace, String schema, String auth) {
        init(namespace, schema, auth);
    }

    private void init(String namespace, String schema, String auth) {
        logger.info("Initializing ...");

        this.namespace = StringUtils.isNotEmpty(namespace) ? namespace : DEFAULT_NAMESPACE;
        this.schema = StringUtils.isNotEmpty(schema) ? schema : DEFAULT_SCHEMA;
        this.auth = StringUtils.isNotEmpty(auth) ? auth : DEFAULT_AUTH;

        String connectString = DEFAULT_CONNECT_STRING;
        int connectionTimeoutMs = DEFAULT_CONNECTION_TIMEOUT;
        int sessionTimeoutMs = DEFAULT_SESSION_TIMEOUT;
        boolean canBeReadOnly = DEFAULT_READ_ONLY;

        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000,
                Integer.MAX_VALUE);

        CuratorFrameworkFactory.Builder builder = CuratorFrameworkFactory.builder();
        builder.connectString(connectString).connectionTimeoutMs(connectionTimeoutMs)
                .sessionTimeoutMs(sessionTimeoutMs).canBeReadOnly(canBeReadOnly)
                .retryPolicy(retryPolicy).namespace(this.namespace);
        if (!StringUtils.isEmpty(this.schema) && !StringUtils.isEmpty(this.auth)) {
            builder.aclProvider(getACLProvider()).authorization(this.schema, this.auth.getBytes());
        }
        this.client = builder.build();
        this.client.start();
    }

    private ACLProvider getACLProvider() {
        ACLProvider aclProvider = new ACLProvider() {
            List<ACL> lstACL = new ArrayList<>();

            @Override
            public List<ACL> getDefaultAcl() {
                try {
                    if (StringUtils.isNotEmpty(schema)
                            && StringUtils.isNotEmpty(auth)) {
                        ACL acl = new ACL(ZooDefs.Perms.ALL,
                                new Id(schema,
                                        DigestAuthenticationProvider.generateDigest(auth)));
                        lstACL.add(acl);
                    }
                } catch (NoSuchAlgorithmException e) {
                    e.printStackTrace();
                }
                return lstACL;
            }

            @Override
            public List<ACL> getAclForPath(String path) {
                return lstACL;
            }
        };
        aclProvider.getDefaultAcl();

        return aclProvider;
    }

    @Override
    public void subscribe(URL url) {
        this.url = url;
        this.watcher = new ZookeeperWatcher(this);
        String path = url.getPath();
        try {
            watched(path);
            watchedChildren(path);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void watched(String path) throws Exception {
        createNode(path);

        NodeCache nodeCache = new NodeCache(client, path);
        nodeCache.start();

        NodeCacheListener listener = watcher;
        nodeCache.getListenable().addListener(listener);
    }

    public void watchedChildren(String path) throws Exception {
        createNode(path);

        PathChildrenCache childrenCache = new PathChildrenCache(client, path, true);
        childrenCache.start();

        PathChildrenCacheListener listener = watcher;
        childrenCache.getListenable().addListener(listener);
    }

    public void createNode(String path) throws Exception {
        createNode(path, null);
    }

    public void createNode(String path, byte[] data) throws Exception {
        Stat stat = client.checkExists().forPath(path);
        if (stat == null) {
            client.create().creatingParentsIfNeeded().forPath(path, data);
        }
    }

    @Override
    public URL getURL() {
        return this.url;
    }

    @Override
    public void addListener(NotifyListener listener) {
        this.listener = listener;
    }

    public NotifyListener getListener() {
        return listener;
    }

    public CuratorFramework getClient() {
        return client;
    }

}
