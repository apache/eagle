/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *    http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.eagle.security.auditlog;

import java.util.*;

/**
 * pattern match supports "within" clause, which means if the first and last event come within this period, it is triggered
 * We don't include "within" into user command patter match because it is hard to estimate the period as some operations may need long time
 */
public class HdfsUserCommandPatternsConstant {
    /**
     * hdfs dfs -appendToFile /tmp/private /tmp/private
     * 2015-11-19 23:57:02,934 INFO FSNamesystem.audit: allowed=true ugi=root (auth:SIMPLE) ip=/10.0.2.15 cmd=getfileinfo src=/tmp/private dst=null perm=null proto=rpc
     * 2015-11-19 23:57:03,046 INFO FSNamesystem.audit: allowed=true ugi=root (auth:SIMPLE) ip=/10.0.2.15 cmd=append src=/tmp/private dst=null perm=null proto=rpc
     * 2015-11-19 23:57:03,118 INFO FSNamesystem.audit: allowed=true ugi=root (auth:SIMPLE) ip=/10.0.2.15 cmd=getfileinfo src=/tmp/private dst=null perm=null proto=rpc
     */
    public static String APPEND_PATTERN = "every a = eventStream[cmd=='getfileinfo'] " +
            "-> b = eventStream[cmd=='append' and user==a.user and src==a.src] " +
            "-> c = eventStream[cmd=='getfileinfo'and user==a.user and src==a.src] ";

    public static SortedMap<String, String> APPEND_SIDDHI_OUTPUT_SELECTOR = new TreeMap<String, String>() {{
        put("timestamp", "a.timestamp");
        put("src", "a.src");
        put("dst", "a.dst");
        put("host", "a.host");
        put("allowed", "c.allowed");
        put("user", "a.user");
    }};

    public static SortedMap<String, String> APPEND_OUTPUT_MODIFIER = new TreeMap<String, String>() {{
        put("cmd", "user:appendToFile");
    }};

    /**
     * hdfs dfs -cat /tmp/private
     * hdfs dfs -checksum /tmp/private
     * hdfs dfs -copyToLocal /tmp/private /tmp/private1
     * hdfs dfs -get /tmp/private /tmp/private2
     * hdfs dfs -tail /tmp/private
     * hdfs dfs -text /tmp/private
     * 2015-11-19 23:47:28,922 INFO FSNamesystem.audit: allowed=true ugi=root (auth:SIMPLE) ip=/10.0.2.15 cmd=getfileinfo src=/tmp/private dst=null perm=null proto=rpc
     * 2015-11-19 23:47:29,026 INFO FSNamesystem.audit: allowed=true ugi=root (auth:SIMPLE) ip=/10.0.2.15 cmd=open src=/tmp/private dst=null perm=null proto=rpc
     */

    public static String READ_PATTERN = "every a = eventStream[cmd=='getfileinfo'] " +
            "-> b = eventStream[cmd=='open' and user==a.user and src==a.src] ";

    public static SortedMap<String, String> READ_SIDDHI_OUTPUT_SELECTOR = new TreeMap<String, String>() {{
        put("timestamp", "a.timestamp");
        put("src", "a.src");
        put("dst", "a.dst");
        put("host", "a.host");
        put("allowed", "a.allowed");
        put("user", "a.user");
    }};

    public static SortedMap<String, String> READ_OUTPUT_MODIFIER = new TreeMap<String, String>() {{
        put("cmd", "user:read");
    }};

    /**
     * hdfs dfs -copyFromLocal -f /tmp/private1 /tmp/private
     * 2015-11-20 00:06:47,090 INFO FSNamesystem.audit: allowed=true ugi=root (auth:SIMPLE) ip=/10.0.2.15 cmd=getfileinfo src=/tmp/private dst=null perm=null proto=rpc
     * 2015-11-20 00:06:47,185 INFO FSNamesystem.audit: allowed=true ugi=root (auth:SIMPLE) ip=/10.0.2.15 cmd=getfileinfo src=/tmp/private._COPYING_ dst=null perm=null proto=rpc
     * 2015-11-20 00:06:47,254 INFO FSNamesystem.audit: allowed=true ugi=root (auth:SIMPLE) ip=/10.0.2.15 cmd=create src=/tmp/private._COPYING_ dst=null perm=root:hdfs:rw-r--r-- proto=rpc
     * 2015-11-20 00:06:47,289 INFO FSNamesystem.audit: allowed=true ugi=root (auth:SIMPLE) ip=/10.0.2.15 cmd=getfileinfo src=/tmp/private._COPYING_ dst=null perm=null proto=rpc
     * 2015-11-20 00:06:47,609 INFO FSNamesystem.audit: allowed=true ugi=root (auth:SIMPLE) ip=/10.0.2.15 cmd=delete src=/tmp/private dst=null perm=null proto=rpc
     * 2015-11-20 00:06:47,624 INFO FSNamesystem.audit: allowed=true ugi=root (auth:SIMPLE) ip=/10.0.2.15 cmd=rename src=/tmp/private._COPYING_ dst=/tmp/private perm=root:hdfs:rw-r--r-- proto=rpc
     */
    public static String COPYFROMLOCAL_PATTERN = "every a = eventStream[cmd=='getfileinfo'] " +
            "-> b = eventStream[cmd=='getfileinfo' and user==a.user and src==str:concat(a.src,'._COPYING_')] " +
            "-> c = eventStream[cmd=='create' and user==a.user and src==b.src] " +
            "-> d = eventStream[cmd=='getfileinfo' and user==a.user and src==b.src] " +
            "-> e = eventStream[cmd=='delete' and user==a.user and src==a.src] " +
            "-> f = eventStream[cmd=='rename' and user==a.user and src==b.src and dst==a.src] ";

    public static SortedMap<String, String> COPYFROMLOCAL_SIDDHI_OUTPUT_SELECTOR = new TreeMap<String, String>() {{
        put("timestamp", "a.timestamp");
        put("dst", "a.src");
        put("host", "a.host");
        put("allowed", "a.allowed");
        put("user", "a.user");
    }};

    public static SortedMap<String, String> COPYFROMLOCAL_OUTPUT_MODIFIER = new TreeMap<String, String>() {{
        put("cmd", "user:copyFromLocal");
    }};
}
