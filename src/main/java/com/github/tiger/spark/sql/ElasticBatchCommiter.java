package com.github.tiger.spark.sql;

import com.github.tiger.spark.elastic.BasisCurdTransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.Serializable;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author liuhongming
 */
public class ElasticBatchCommiter implements Serializable {

    private int limited = 100;

    private AtomicInteger modCount = new AtomicInteger(0);

    private List<Info> cache = new ArrayList<>();

    private String index;

    private String type;

    private static BasisCurdTransportClient esClient;

    public ElasticBatchCommiter(String clusterName, String index, String type) {

        this.index = index;
        this.type = type;

        Settings settings = Settings.builder()
                .put("cluster.name", clusterName)
                .put("client.transport.sniff", true).build();
        try {
            InetSocketTransportAddress[] transports = new InetSocketTransportAddress[]{
                    new InetSocketTransportAddress(InetAddress.getByName("175.63.101.107"), 9300),
                    new InetSocketTransportAddress(InetAddress.getByName("175.63.101.108"), 9300),
                    new InetSocketTransportAddress(InetAddress.getByName("175.63.101.109"), 9300)};

            esClient = new BasisCurdTransportClient(settings, transports);

        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
    }

    public boolean checkCount() {
        int count = modCount.get();
        if (count > 0 && (count % limited == 0)) {
            return true;
        }
        return false;
    }

    public void commit(String id, XContentBuilder source) {

        esClient.saveDocument(index, type, id, source);

//        if (checkCount()) {
//            for (Info info : cache) {
//                esClient.saveDocument(index, type, info.getId(), info.getSource());
//            }
//            cache.clear();
//            modCount.getAndSet(0);
//        } else {
//            cache.add(new Info(id, source));
//            modCount.addAndGet(1);
//        }

    }

    class Info {

        private String id;

        private XContentBuilder source;

        public Info(String id, XContentBuilder source) {
            this.id = id;
            this.source = source;
        }

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public XContentBuilder getSource() {
            return source;
        }

        public void setSource(XContentBuilder source) {
            this.source = source;
        }
    }

}
