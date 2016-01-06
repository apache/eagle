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

package org.apache.eagle.state.deltaevent;

import kafka.admin.AdminUtils;
import kafka.api.TopicMetadata;
import org.I0Itec.zkclient.ZkClient;
import scala.collection.JavaConversions;
import java.util.List;
import java.util.Properties;

/**
 * get number of partitions for a topic
 */
public class TestGetNumPartitions {
    public static void main(String[] args) throws Exception{
        ZkClient zkClient = new ZkClient("localhost:2181");
        try {
            AdminUtils.createTopic(zkClient, "testTopic", 3, 1, new Properties());
        }catch(Exception ex){
            // continue to run
        }
        TopicMetadata metadata = AdminUtils.fetchTopicMetadataFromZk("testTopic", zkClient);
        List partitions = scala.collection.JavaConversions.seqAsJavaList(metadata.partitionsMetadata());
        System.out.println(partitions.size());
    }
}
