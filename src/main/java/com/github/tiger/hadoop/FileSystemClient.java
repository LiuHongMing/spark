package com.github.tiger.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URI;

/**
 * FileSystem 工具类
 * <p>
 * 实现对 local、hdfs 文件操作
 *
 * @author liuhongming
 */
public class FileSystemClient {

    private static final Logger logger = LoggerFactory.getLogger(FileSystemClient.class);

    public static void copyFileToHdfs(Configuration conf, String src, String dest) {

        try (
                InputStream in = new BufferedInputStream(new FileInputStream(src));

                FileSystem fs = FileSystem.get(URI.create(dest), conf);

                OutputStream out = fs.create(new Path(dest),

                        () -> logger.info("设定缓存区大小容量，完成上传文件！"));) {

            IOUtils.copyBytes(in, out, 4096, true);

        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }
    }

}
