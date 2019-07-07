package com.github.tiger.spark.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.io.Serializable;

/**
 * @author liuhongming
 */
public class HBaseReadWriter implements Serializable {

    private static Connection conn;

    static {
        Configuration hbaseConfig = HBaseConfiguration.create();
        hbaseConfig.set("hbase.zookeeper.quorum", "spark-220,spark-221,spark-223");
        try {
            conn = ConnectionFactory.createConnection(hbaseConfig);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void writeData(String tableName,
                                 String rowKey, String family,
                                 String qualifier, String value) {
        try (Table table = conn.getTable(TableName.valueOf(tableName))) {
            Put put = new Put(rowKey.getBytes());
            put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(value));
            table.put(put);
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    public static String readData(String tableName, String family, String qualifier) {
        try (Table table = conn.getTable(TableName.valueOf(tableName))) {
            Scan scan = new Scan();
            scan.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));

            ResultScanner resultScanner = table.getScanner(scan);
            Result result = resultScanner.next();
            Cell cell = result.getColumnLatestCell(Bytes.toBytes(family), Bytes.toBytes(qualifier));
            String ret = Bytes.toString(cell.getValueArray(),
                    cell.getValueOffset(), cell.getValueLength());
            return ret;
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        return "";
    }

    public static void main(String[] args) {
//        HBaseReadWriter.writeData("user_behavior_jobs",
//                "CC000796385J90000784000", "job_num",
//                "CC000796385J90000784000", "new");

        String value = HBaseReadWriter.readData("user_behavior_jobs",
                "job_num", "CC000796385J90000784000");
        System.out.println(value);
    }

}
