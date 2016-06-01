package org.apache.eagle.alert.config;

import java.io.Serializable;

/**
 * Memory representation of key zookeeper configurations
 */
public class ZKConfig implements Serializable{
    private static final long serialVersionUID = -1287231022807492775L;

    public String zkQuorum;
    public String zkRoot;
    public int zkSessionTimeoutMs;
    public int connectionTimeoutMs;
    public int zkRetryTimes;
    public int zkRetryInterval;
}
