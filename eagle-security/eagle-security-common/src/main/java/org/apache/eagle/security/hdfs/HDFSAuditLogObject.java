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

package org.apache.eagle.security.hdfs;

public class HDFSAuditLogObject {
    public long timestamp;
    public String host;
    public Boolean allowed;
    public String user;
    public String cmd;
    public String src;
    public String dst;

    public static final String HDFS_TIMESTAMP_KEY = "timestamp";
    public static final String HDFS_HOST_KEY = "host";
    public static final String HDFS_ALLOWED_KEY = "allowed";
    public static final String HDFS_USER_KEY = "user";
    public static final String HDFS_CMD_KEY = "cmd";
    public static final String HDFS_SRC_KEY = "src";
    public static final String HDFS_DST_KEY = "dst";
}
