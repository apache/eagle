/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.eagle.hadoop.jmx;

import java.util.regex.Pattern;

public class HadoopJmxConstant {

    public static final String HDFS_INSTANCE_SERVICE_NAME = "HdfsServiceInstance";
    public static final String HBASE_INSTANCE_SERVICE_NAME = "HbaseServiceInstance";
    public static final String MR_INSTANCE_SERVICE_NAME = "MRServiceInstance";
    public static final String JN_INSTANCE_SERVICE_NAME = "JNServiceInstance";
    public static final String GENERIC_METRIC_SERVICE = "GenericMetricService";

    public static final String FSNAMESYSTEM_BEAN = "Hadoop:service=NameNode,name=FSNamesystem";
    public static final String NAMENODEINFO_BEAN = "Hadoop:service=NameNode,name=NameNodeInfo";
    public static final String FSNAMESYSTEMSTATE_BEAN = "Hadoop:service=NameNode,name=FSNamesystemState";
    public static final String FS_HASTATE_TAG = "tag.HAState";
    public static final String FS_HOSTNAME_TAG = "tag.Hostname";

    public static final String PLACE_HOLDER = "PLACEHOLDER";

    public static final int DEFAULT_READ_TIMEOUT = 30 * 60 * 1000; // in milliseconds
    public static final Pattern HTTP_HOST_MATCH_PATTERN = Pattern.compile("^https?://(.+?):-?(\\d+)/?");
    public static final Pattern HTTP_HOST_MATCH_PATTERN_2 = Pattern.compile("^//(.+?):-?(\\d+)/");
    public static final Pattern LOCAL_HOST_MATCH_PATTERN = Pattern.compile("^///(.+?):-?(\\d+)/");

    public static final String HA_TOTAL_FORMAT = "hadoop.%.hastate.total.count";
    public static final String HA_ACTIVE_FORMAT = "hadoop.%.hastate.active.count";
    public static final String HA_STANDBY_FORMAT = "hadoop.%.hastate.standby.count";
    public static final String HA_FAILED_FORMAT = "hadoop.%.hastate.failed.count";

    public static final String SITE_TAG = "site";
    public static final String RACK_TAG = "rack";
    public static final String HOSTNAME_TAG = "hostname";
    public static final String CATEGORY_TAG = "category";
    public static final String ROLE_TAG = "role";
    public static final String COMPONENT_TAG = "component";

    public static final String NAME_NODE_ROLE = "namenode";
    public static final String DATA_NODE_ROLE = "datanode";
    public static final String JOURNAL_NODE_ROLE = "journalnode";
    public static final String RESOURCE_MANAGER_ROLE = "resourcemanager";
    public static final String NODE_MANAGER_ROLE = "nodemanager";
    public static final String HISTORY_SERVER_ROLE = "historyserver";
    public static final String REGIONSERVER_ROLE = "regionserver";
    public static final String HMASTER_ROLE = "hmaster";

    // Status definitions for MASTER node
    public static final String ACTIVE_STATE = "active";
    public static final String STANDBY_STATE = "standby";

    // Status definitions for data node
    public static final String DATA_NODE_LIVE_STATUS = "live";
    public static final String DATA_NODE_DEAD_STATUS = "dead";
    public static final String DATA_NODE_LIVE_DECOMMISSIONED_STATUS = "live_decommissioned";
    public static final String DATA_NODE_DEAD_DECOMMISSIONED_STATUS = "dead_decommissioned";
    public static final String DATA_NODE_DEAD_NOT_DECOMMISSIONED_STATUS = "dead_not_decommissioned";

    // Status definitions for node manager
    public static final String NODE_MANAGER_RUNNING_STATUS = "running";
    public static final String NODE_MANAGER_LOST_STATUS = "lost";
    public static final String NODE_MANAGER_UNHEALTHY_STATUS = "unhealthy";

    // Status definitions for hbase regionserver
    public static final String REGIONSERVER_LIVE_STATUS = "live";
    public static final String REGIONSERVER_DEAD_STATUS = "dead";

    // metrics
    public static final String METRIC_LIVE_RATIO_NAME_FORMAT = "hadoop.%s.live.ratio";
    public static final String METRIC_LIVE_COUNT_NAME_FORMAT = "hadoop.%s.live.count";

    // stream fields
    // field constants
    public static final String SERVICE_NAME_FIELD = "serviceName";
    public static final String TOPOLOGY_DATA_FIELD = "topologyData";

    public enum MetricType {
        METRIC, RESOURCE
    }
}
