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

package org.apache.eagle.service.application;


public class AppManagerConstants {
    public final static String SITE_TAG = "site";
    public final static String APPLICATION_TAG = "application";
    public final static String OPERATION_TAG = "operation";
    public final static String OPERATION_ID_TAG = "operationID";
    public final static String TOPOLOGY_TAG = "topology";
    public final static String FULLNAME = "fullName";
    public final static String APPLICATION_ID = "id";

    public final static String CLUSTER_TYPE = "envContextConfig.mode";

    public final static String EAGLE_CLUSTER_STORM = "storm";
    public final static String EAGLE_CLUSTER_SPARK = "spark";

    public final static String APP_WORKER_THREADS_CORE_SIZE = "appWorkerThreadPoolCoreSize";
    public final static String APP_WORKER_THREADS_MAX_SIZE = "appWorkerThreadPoolMaxSize";

    public final static String APP_COMMAND_LOADER_INTERVAL = "appCommandLoaderInterval";
    public final static String APP_HEALTH_CHECK_INTERVAL = "appHealthCheckInterval";

}
