/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.eagle.common.config;

public final class EagleConfigConstants {
    public static final String SERVICE_ENV = "service.env";
    public static final String SERVICE_HOST = "service.host";
    public static final String SERVICE_PORT = "service.port";
    public static final String SERVICE_HBASE_ZOOKEEPER_QUORUM = "storage.hbaseZookeeperQuorum";
    public static final String SERVICE_HBASE_ZOOKEEPER_PROPERTY_CLIENTPORT = "storage.hbaseZookeeperPropertyClientPort";
    public static final String SERVICE_ZOOKEEPER_ZNODE_PARENT = "storage.zookeeperZnodeParent";
    public static final String SERVICE_HBASE_CLIENT_IPC_POOL_SIZE = "storage.hbaseClientIpcPoolSize";
    public static final String SERVICE_STORAGE_TYPE = "storage.storageType";
    public static final String SERVICE_COPROCESSOR_ENABLED = "storage.coprocessorEnabled";
    public static final String SERVICE_TABLE_NAME_PREFIXED_WITH_ENVIRONMENT = "storage.tableNamePrefixedWithEnvironment";
    public static final String SERVICE_HBASE_CLIENT_SCAN_CACHE_SIZE = "storage.hbaseClientScanCacheSize";
    public static final String SERVICE_THREADPOOL_CORE_SIZE = "storage.threadpoolCoreSize";
    public static final String SERVICE_THREADPOOL_MAX_SIZE = "storage.threadpoolMaxSize";
    public static final String SERVICE_THREADPOOL_SHRINK_SIZE = "storage.threadpoolShrinkSize";
    public static final String SERVICE_AUDITING_ENABLED = "storage.auditEnabled";

    public static final String EAGLE_TIME_ZONE = "service.timezone";
    public static final String DEFAULT_EAGLE_TIME_ZONE = "UTC";

    public static final int DEFAULT_THREAD_POOL_CORE_SIZE = 10;
    public static final int DEFAULT_THREAD_POOL_MAX_SIZE = 20;
    public static final long DEFAULT_THREAD_POOL_SHRINK_TIME = 60000L;
    public static final String DEFAULT_SERVICE_HOST = "localhost";
    public static final String DEFAULT_STORAGE_TYPE = "hbase";
    public static final int DEFAULT_SERVICE_PORT = 8080;
    public static final String DEFAULT_ZOOKEEPER_ZNODE_PARENT = "/hbase-unsecure";

    public static final String EAGLE_PROPS="eagleProps";
    public static final String EAGLE_SERVICE = "eagleService";
    public static final String HOST = "host";
    public static final String PORT = "port";
    public static final String USERNAME = "username";
    public static final String PASSWORD = "password";

    public static final String SITE = "site";
    @Deprecated
    public static final String DATA_SOURCE = "dataSource";
    public static final String APPLICATION = "application";

    public static final String WEB_CONFIG = "web";
    public static final String APP_CONFIG = "app";
    public static final String CLASSIFICATION_CONFIG = "classification";

    public static final String LOCAL_MODE = "local";
    public static final String CLUSTER_MODE = "cluster";
}