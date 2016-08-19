/*
 *
 *    Licensed to the Apache Software Foundation (ASF) under one or more
 *    contributor license agreements.  See the NOTICE file distributed with
 *    this work for additional information regarding copyright ownership.
 *    The ASF licenses this file to You under the Apache License, Version 2.0
 *    (the "License"); you may not use this file except in compliance with
 *    the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 *
 */

package org.apache.eagle.security.partition;

import org.apache.eagle.dataproc.impl.storm.partition.PartitionAlgorithm;
import org.apache.eagle.dataproc.impl.storm.partition.Weight;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class GreedyPartitionAlgorithm implements PartitionAlgorithm {

    private static final Logger LOG = LoggerFactory.getLogger(GreedyPartitionAlgorithm.class);

    public void printWeightTable(PriorityQueue<Bucket> queue) {
        double total = 0;
        Iterator<Bucket> iter = queue.iterator();
        while (iter.hasNext()) {
            total += iter.next().value;
        }
        StringBuilder sb = new StringBuilder();
        iter = queue.iterator();
        while (iter.hasNext()) {
            sb.append(iter.next().value / total + ",");
        }
        sb.deleteCharAt(sb.length()-1);
        LOG.info("Weights: " + sb.toString());
    }

    public HashMap<String, Integer> partition(List<Weight> weights, int k) {
        PriorityQueue<Bucket> queue = new PriorityQueue<>(k, new BucketComparator());
        HashMap<String, Integer> ret = new HashMap<>();
        // Initialize the queue
        for (int i = 0; i < k; i++) {
            queue.add(new Bucket(i, 0.0));
        }
        int n = weights.size();
        for (int i = 0; i < n; i++) {
            Bucket bucket = queue.poll();
            bucket.value = bucket.value + weights.get(i).value;
            queue.add(bucket);
            ret.put(weights.get(i).key, bucket.bucketNum);
        }
        printWeightTable(queue);
        return ret;
    }
}
