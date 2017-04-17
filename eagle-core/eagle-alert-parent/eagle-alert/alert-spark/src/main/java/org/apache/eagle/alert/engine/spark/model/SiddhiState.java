/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.eagle.alert.engine.spark.model;

import kafka.message.MessageAndMetadata;
import org.apache.eagle.alert.engine.spark.accumulator.MapToMapAccum;
import org.apache.spark.Accumulator;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

public class SiddhiState implements Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(SiddhiState.class);
    private static final long serialVersionUID = -2180409299871716057L;
    private AtomicReference<Map<Integer, Map<String, byte[]>>> siddhiSnapshot = new AtomicReference<>();
    private Accumulator<Map<Integer, Map<String, byte[]>>> siddhiSnapShotAccum;

    public SiddhiState() {
    }

    public void initailSiddhiState(JavaRDD<MessageAndMetadata<String, String>> rdd) {
        Accumulator<Map<Integer, Map<String, byte[]>>> siddhiSnapShotAccum = StateInstance.getInstance(new JavaSparkContext(rdd.context()), "siddhiSnapShotState", new MapToMapAccum());
        this.siddhiSnapShotAccum = siddhiSnapShotAccum;
    }

    public void recover(JavaRDD<MessageAndMetadata<String, String>> rdd) {
        initailSiddhiState(rdd);
        siddhiSnapshot.set(siddhiSnapShotAccum.value());
        LOG.debug("---------siddhiSnapshot----------" + siddhiSnapshot.get());
    }

    public void store(byte[] snapShot, String boltId, int partitionNum) {
        Map<Integer, Map<String, byte[]>> siddhiSnapShot = new HashMap<>();
        Map<String, byte[]> boltIdToSnapShot = new HashMap<>();
        boltIdToSnapShot.put(boltId, snapShot);
        siddhiSnapShot.put(partitionNum, boltIdToSnapShot);
        LOG.debug("---------store---siddhiSnapshot----------" + siddhiSnapShot);
        siddhiSnapShotAccum.add(siddhiSnapShot);
    }

    public byte[] getSiddhiSnapShotByBoltIdAndPartitionNum(String boltId, int partitionNum) {
        Map<Integer, Map<String, byte[]>> partitionToSnapShot = siddhiSnapshot.get();
        LOG.debug("---SiddhiState----getSiddhiSnapShotByPartition----------" + (partitionToSnapShot));
        Map<String, byte[]> boltIdToSnapShot = partitionToSnapShot.get(partitionNum);
        byte[] siddhiSnapshot = null;
        if (boltIdToSnapShot != null) {
            siddhiSnapshot = boltIdToSnapShot.get(boltId);
        }
        return siddhiSnapshot;
    }
}
