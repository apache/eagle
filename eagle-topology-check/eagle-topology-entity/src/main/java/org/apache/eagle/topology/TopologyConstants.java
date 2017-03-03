/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package org.apache.eagle.topology;

import java.util.regex.Pattern;

public class TopologyConstants {

    public static final String SYSTEM_INSTANCE_SERVICE_NAME = "SystemServiceInstance";
    public static final String HDFS_INSTANCE_SERVICE_NAME = "HdfsServiceInstance";
    public static final String HBASE_INSTANCE_SERVICE_NAME = "HbaseServiceInstance";
    public static final String MR_INSTANCE_SERVICE_NAME = "MRServiceInstance";
    public static final String JN_INSTANCE_SERVICE_NAME = "JNServiceInstance";
    public static final String GENERIC_METRIC_SERVICE = "GenericMetricService";

    public static final int DEFAULT_READ_TIMEOUT = 30 * 60 * 1000; // in milliseconds
    public static final Pattern HTTP_HOST_MATCH_PATTERN = Pattern.compile("^https?://(.+?):-?(\\d+)/?");
    public static final Pattern HTTP_HOST_MATCH_PATTERN_2 = Pattern.compile("^//(.+?):-?(\\d+)/");
    public static final Pattern LOCAL_HOST_MATCH_PATTERN = Pattern.compile("^///(.+?):-?(\\d+)/");

    public static final String SITE_TAG = "site";
    public static final String RACK_TAG = "rack";
    public static final String HOSTNAME_TAG = "hostname";
    public static final String CATEGORY_TAG = "category";
    public static final String ROLE_TAG = "role";

    public static final String NAME_NODE_ROLE = "namenode";
    public static final String DATA_NODE_ROLE = "datanode";
    public static final String JOURNAL_NODE_ROLE = "journalnode";
    public static final String RESOURCE_MANAGER_ROLE = "resourcemanager";
    public static final String NODE_MANAGER_ROLE = "nodemanager";
    public static final String HISTORY_SERVER_ROLE = "historyserver";
    public static final String REGIONSERVER_ROLE = "regionserver";
    public static final String HMASTER_ROLE = "hmaster";
    public static final String SYSTEM_ROLE = "system";

    // Status definitions for namenode
    public static final String NAME_NODE_ACTIVE_STATUS = "active";
    public static final String NAME_NODE_STANDBY_STATUS = "standby";

    // Status definitions for data node
    public static final String DATA_NODE_LIVE_STATUS = "live";
    public static final String DATA_NODE_DEAD_STATUS = "dead";
    public static final String DATA_NODE_LIVE_DECOMMISSIONED_STATUS = "live_decommissioned";
    public static final String DATA_NODE_DEAD_DECOMMISSIONED_STATUS = "dead_decommissioned";
    public static final String DATA_NODE_DEAD_NOT_DECOMMISSIONED_STATUS = "dead_not_decommissioned";

    // Status definitions for resource manager
    public static final String RESOURCE_MANAGER_ACTIVE_STATUS = "active";
    public static final String RESOURCE_MANAGER_INACTIVE_STATUS = "inactive";

    // Status definitions for node manager
    public static final String NODE_MANAGER_RUNNING_STATUS = "running";
    public static final String NODE_MANAGER_LOST_STATUS = "lost";
    public static final String NODE_MANAGER_UNHEALTHY_STATUS = "unhealthy";

    // Status definitions for hbase regionserver
    public static final String REGIONSERVER_LIVE_STATUS = "live";
    public static final String REGIONSERVER_DEAD_STATUS = "dead";

    // Status definitions for hbase hmaster
    public static final String HMASTER_ACTIVE_STATUS = "active";
    public static final String HMASTER_STANDBY_STATUS = "standby";

    // metrics
    public static final String METRIC_LIVE_RATIO_NAME_FORMAT = "hadoop.%s.live.ratio";
    public static final String METRIC_LIVE_COUNT_NAME_FORMAT = "hadoop.%s.live.count";

    // stream fields
    // field constants
    public static final String SERVICE_NAME_FIELD = "serviceName";
    public static final String TOPOLOGY_DATA_FIELD = "topologyData";
    
    public static final String COLON = ":";

    public enum HadoopVersion {
        V2
    }

    public enum TopologyType {
        HDFS,
        HBASE,
        MR
    }
}

