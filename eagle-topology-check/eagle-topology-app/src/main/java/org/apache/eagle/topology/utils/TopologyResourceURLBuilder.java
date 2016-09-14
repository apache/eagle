/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.eagle.topology.utils;

import org.apache.eagle.app.utils.PathResolverHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TopologyResourceURLBuilder {
    private static final Logger LOG = LoggerFactory.getLogger(TopologyResourceURLBuilder.class);

    private static final String JMX_URL = "jmx?anonymous=true";
    public static final String HDFS_JMX_FS_SYSTEM_BEAN = "Hadoop:service=NameNode,name=FSNamesystem";
    public static final String HDFS_JMX_NAMENODE_INFO = "Hadoop:service=NameNode,name=NameNodeInfo";

    public static String buildFSNamesystemURL(String url) {
        return PathResolverHelper.buildUrlPath(url, "&qry=" + JMX_URL + HDFS_JMX_FS_SYSTEM_BEAN);
    }

    public static String buildNamenodeInfo(String url) {
        return PathResolverHelper.buildUrlPath(url, "&qry=" + JMX_URL + HDFS_JMX_NAMENODE_INFO);
    }


}
