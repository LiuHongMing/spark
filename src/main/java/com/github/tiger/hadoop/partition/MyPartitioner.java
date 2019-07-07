package com.github.tiger.hadoop.partition;


import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

/**
 * 自定义分区器
 *
 * @author liuhongming
 */
public class MyPartitioner extends HashPartitioner {

    @Override
    public int getPartition(Object key, Object value, int numReduceTasks) {
        return super.getPartition(key, value, numReduceTasks);
    }
}
