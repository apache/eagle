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

    public static final String HDFS_INSTANCE_SERVICE_NAME = "HdfsServiceInstance";
    public static final String HBASE_INSTANCE_SERVICE_NAME = "HbaseServiceInstance";
    public static final String MR_INSTANCE_SERVICE_NAME = "MRServiceInstance";
    public static final String NODE_STATUS_SERVICE_NAME = "NodeStatus";

    public static final String HDFS_SERVICE_SERVICE_NAME = "HdfsService";
    public static final String HBASE_SERVICE_SERVICE_NAME = "HbaseService";
    public static final String MR_SERVICE_SERVICE_NAME = "MRService";

    public static final String HDFS_INSTANCE_RESOURCE_URL_PATH = "/topologies/hdfs_instance";
    public static final String HBASE_INSTANCE_RESOURCE_URL_PATH = "/topologies/hbase_instance";
    public static final String MR_INSTANCE_RESOURCE_URL_PATH = "/topologies/mrservice_instance";

    public static final String NODE_STATUS_RESOURCE_URL_PATH = "/topologies/nodes";

    public static final String HDFS_SERVICE_RESOURCE_URL_PATH = "/topologies/hdfs_service";
    public static final String HBASE_SERVICE_RESOURCE_URL_PATH = "/topologies/hbase_service";
    public static final String MR_SERVICE_RESOURCE_URL_PATH = "/topologies/mrservice_service";


    public static final int DEFAULT_READ_TIMEOUT = 30 * 60 * 1000; // in milliseconds
    public static final Pattern HTTP_HOST_MATCH_PATTERN = Pattern.compile("^https?://(.+?):-?(\\d+)/");
    public static final Pattern HTTP_HOST_MATCH_PATTERN_2 = Pattern.compile("^//(.+?):-?(\\d+)/");
    public static final Pattern LOCAL_HOST_MATCH_PATTERN = Pattern.compile("^///(.+?):-?(\\d+)/");


    public enum HadoopVersion {
        V1,
        V2
    }

    public enum NamenodeState {
        active,
        standby
    }

    public enum TopologyType {
        HDFS,
        HBASE,
        MR
    }

    public enum TopologySourceType {
        WEB,
        JMX,
        YARN
    }

}

