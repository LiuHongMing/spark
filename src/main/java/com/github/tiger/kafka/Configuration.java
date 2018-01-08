package com.github.tiger.kafka;

import com.github.tiger.kafka.common.URL;
import com.github.tiger.kafka.consumer.ConsumerMain;
import com.github.tiger.kafka.consumer.handler.HttpDispatcher;
import com.github.tiger.kafka.listener.NotifyListener;
import com.github.tiger.kafka.registry.Registry;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.zpcampus.commonutil.gson.GsonUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * @author liuhongming
 */
public class Configuration implements NotifyListener {

    public static final Logger logger = LoggerFactory.getLogger(Configuration.class);

    public static Map<String, Object> all = Maps.newConcurrentMap();

    public static Map<String, Object> allBiz = Maps.newConcurrentMap();

    public static List<String> allStartupBiz = Lists.newArrayList();

    public static List<TopicBean> allTopic = Lists.newArrayList();

    private Registry registry;

    public Configuration(Registry registry) {
        this.registry = registry;
        init();
    }

    private void init() {
        this.registry.addListener(this);
    }

    public void sync(URL subscribeUrl) {
        registry.subscribe(subscribeUrl);
    }

    public void update(Map<String, Object> config) {
        all.putAll(config);

        Object biz = all.get("BusinessKeys");
        if (biz instanceof Map) {
            allBiz.putAll((Map) biz);
        }

        Object startupBiz = all.get("StartupBusinessId");
        if (startupBiz instanceof Collection) {
            allStartupBiz.addAll((Collection) startupBiz);
        }

        for (String val : allStartupBiz) {
            String value = (String) allBiz.get(val);

            TopicBean topicBean = new TopicBean();
            topicBean.setExchangName((String) all.get(value + ".Exchang.Name"));
            topicBean.setForwardingUrl((String) all.get(value + ".Forwarding.Url"));
            topicBean.setForwardingUrlParams((String) all.get(value + ".Forwarding.Url.Params"));

            allTopic.add(topicBean);

            doService(topicBean);
        }
    }

    private void doService(TopicBean topicBean) {
        List<String> topicList = Lists.newArrayList(topicBean.getExchangName());
        Splitter.MapSplitter mapSplitter = Splitter.on("&").withKeyValueSeparator("=");
        Map<String, String> parameters = mapSplitter.split(topicBean.getForwardingUrlParams());

        URL url = new URL(topicBean.getForwardingUrl(), new HashMap<>(parameters));
        HttpDispatcher handler = new HttpDispatcher(url);

        ConsumerMain.doReceive(2, topicList, handler);
    }

    @Override
    public void notifyNodeChanged(String path, byte[] data) {
        Map<String, Object> configMap = Maps.newHashMap();
        String content = new String(data);
        boolean isProperties = path.endsWith(".properties");
        if (isProperties) {
            configMap.putAll((Map) GsonUtil.fromJson(content, Properties.class));
        } else {
            configMap.putAll(GsonUtil.fromJson(content, Map.class));
        }

        update(configMap);
    }

}
