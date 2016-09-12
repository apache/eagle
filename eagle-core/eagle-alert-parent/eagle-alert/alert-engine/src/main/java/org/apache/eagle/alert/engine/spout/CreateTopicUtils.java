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
package org.apache.eagle.alert.engine.spout;

import org.slf4j.Logger;

/**
 * normally this is used in unit test for convenience.
 */
public class CreateTopicUtils {

    private static final Logger LOG = org.slf4j.LoggerFactory.getLogger(CreateTopicUtils.class);

    private static final int partitions = 2;
    private static final int replicationFactor = 1;

    public static void ensureTopicReady(String zkQuorum, String topic) {
        //        ZkConnection zkConnection = new ZkConnection(zkQuorum);
        //        ZkClient zkClient = new ZkClient(zkQuorum, 10000, 10000, ZKStringSerializer$.MODULE$);
        ////        ZkUtils zkUtils = new ZkUtils(zkClient, zkConnection, false);
        //        if (!AdminUtils.topicExists(zkClient, topic)) {
        //            LOG.info("create topic " + topic + " with partitions " + partitions + ", and replicationFactor "
        //                    + replicationFactor);
        //            AdminUtils.createTopic(zkClient, topic, partitions, replicationFactor, new Properties());
        //        }
    }
}
