package com.github.tiger.spark.sql;

import com.zpcampus.spark.hbase.HBaseReadWriter;
import com.zpcampus.spark.util.Md5Util;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.sql.ForeachWriter;
import org.apache.spark.sql.Row;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Streaming writer
 *
 * @author liuhongming
 */
public class QueryForeachWriter extends ForeachWriter<Row> implements Serializable {

    private static ElasticBatchCommiter commiter;

    private static Connection conn;

    static {
        commiter = new ElasticBatchCommiter(
                "campus-logs", "spark-streaming", "user-behavior-total");
    }

    private Long start;

    public QueryForeachWriter(long start) {
        this.start = start;
        try {
            Configuration hbaseConfig = HBaseConfiguration.create();
            hbaseConfig.set("hbase.zookeeper.quorum", "spark-220,spark-221,spark-223");
            conn = ConnectionFactory.createConnection(hbaseConfig);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public boolean open(long partitionId, long version) {
        return true;
    }

    @Override
    public void process(Row row) {
        String tableName = "user_behavior_jobs";

        Table table = null;
        try {
            table = conn.getTable(TableName.valueOf(tableName));
        } catch (IOException e) {
            e.printStackTrace();
        }

        if (table != null) {

            String jobNum = row.getString(0);

            String family = "job_num";
            String qualifier = jobNum;

            String newUpdate = "new";
            String oldUpdate = "old";

            String value = HBaseReadWriter.readData(tableName, family, qualifier);
            if (!newUpdate.equals(value)) {
                return;
            }

            Long favorite = row.getLong(1);
            Long view = row.getLong(2);
            Long apply = row.getLong(3);
            Long unfavorite = row.getLong(4);

            Date createDate = new Date(start);
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'+0800'");
            UserBehaviorsSqlDriver.BehaviorEs behavior =
                    new UserBehaviorsSqlDriver.BehaviorEs(
                            jobNum, favorite, view, apply, unfavorite, sdf.format(createDate));
            String id = Md5Util.md5(start + "_" + jobNum);

            String _id = id;
            XContentBuilder _source = behavior.toEsSource();

            commiter.commit(_id, _source);

            Put put = new Put(jobNum.getBytes());
            put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(oldUpdate));
            try {
                table.put(put);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }

    @Override
    public void close(Throwable errorOrNull) {

    }
}
