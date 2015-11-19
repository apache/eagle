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
    public final static String SERVICE_ENV = "eagle.service.env";
    public final static String SERVICE_HOST = "eagle.service.host";
    public final static String SERVICE_PORT = "eagle.service.port";
    public final static String SERVICE_HBASE_ZOOKEEPER_QUORUM = "eagle.service.hbase-zookeeper-quorum";
    public final static String SERVICE_HBASE_ZOOKEEPER_PROPERTY_CLIENTPORT = "eagle.service.hbase-zookeeper-property-clientPort";
    public final static String SERVICE_ZOOKEEPER_ZNODE_PARENT = "eagle.service.zookeeper-znode-parent";
    public final static String SERVICE_HBASE_CLIENT_IPC_POOL_SIZE = "eagle.service.hbase-client-ipc-pool-size";
    public final static String SERVICE_STORAGE_TYPE = "eagle.service.storage-type";
    public final static String SERVICE_COPROCESSOR_ENABLED = "eagle.service.coprocessor-enabled";
    public final static String SERVICE_TABLE_NAME_PREFIXED_WITH_ENVIRONMENT = "eagle.service.table-name-prefixed-with-environment";
    public final static String SERVICE_HBASE_CLIENT_SCAN_CACHE_SIZE = "eagle.service.hbase-client-scan-cache-size";
    public final static String SERVICE_THREADPOOL_CORE_SIZE = "eagle.service.threadpool-core-size";
    public final static String SERVICE_THREADPOOL_MAX_SIZE = "eagle.service.threadpool-max-size";
    public final static String SERVICE_THREADPOOL_SHRINK_SIZE = "eagle.service.threadpool-shrink-size";

    public final static String EAGLE_TIME_ZONE = "eagle.timezone";
    public final static String DEFAULT_EAGLE_TIME_ZONE = "UTC";

    public final static int DEFAULT_THREAD_POOL_CORE_SIZE = 10;
    public final static int DEFAULT_THREAD_POOL_MAX_SIZE = 20;
    public final static long DEFAULT_THREAD_POOL_SHRINK_TIME = 60000L;
    public final static String DEFAULT_SERVICE_HOST = "localhost";
    public final static String DEFAULT_STORAGE_TYPE = "hbase";
    public final static int DEFAULT_SERVICE_PORT = 8080;
    public final static String DEFAULT_ZOOKEEPER_ZNODE_PARENT = "/hbase-unsecure";

    public final static String EAGLE_PROPS="eagleProps";
    public final static String EAGLE_SERVICE = "eagleService";
    public final static String HOST = "host";
    public final static String PORT = "port";
    public final static String USERNAME = "username";
    public final static String PASSWORD = "password";

    public final static String SITE = "site";
    public final static String DATA_SOURCE = "dataSource";
}