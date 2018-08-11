package com.github.tiger.spark.demo;


import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

/**
 * 自定义分区文件
 *
 * @author liuhongming
 */
public class MyPartitioner extends HashPartitioner {

    @Override
    public int getPartition(Object key, Object value, int numReduceTasks) {
        return super.getPartition(key, value, numReduceTasks);
    }
}
