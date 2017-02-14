/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.eagle.health.jobs;

/**
 * Healthy Checker Event/Status Type.
 */
public enum EventType {
    OK,

    UNKNOWN_ERROR,
    TIMEOUT_ERROR,
    EXECUTION_ERROR,
    INTERRUPTED_ERROR,

    MAPRED_JOB_ERROR,

    HDFS_CONNECT_ERROR,
    HDFS_READ_ERROR,
    HDFS_WRITE_ERROR,
    HDFS_DELETE_ERROR,
    HDFS_CREATE_ERROR,

    HBASE_MASTER_NOT_RUNNING,
    HBASE_READ_TABLE_ERROR,
    HBASE_CREATE_TABLE_ERROR,
    HBASE_CREATE_REGION_ERROR,
    HBASE_MOVE_REGION_ERROR,
    HBASE_WRITE_REGION_ERROR,
    HBASE_READ_REGION_ERROR,

    ZOOKEEPER_ERROR,
    ZOOKEEPER_CONNECTION_ERROR,
    ZOOKEEPER_IO_ERROR,
}