package com.github.tiger.kafka.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;

/**
 * @author liuhongming
 */
public class NetUtil {

    private static final Logger log = LoggerFactory.getLogger(NetUtil.class);

    public static InetSocketAddress newSocket(String host, Integer port) throws Exception {
        if (host == null || port == null) {
            throw new Exception("Invalid host or port");
        }

        InetSocketAddress address = new InetSocketAddress(host, port);
        if (address.isUnresolved()) {
            log.warn("DNS resolution failed for {}", host);
        }

        return address;
    }

}
