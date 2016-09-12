/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.eagle.alert.engine.router.impl;

import backtype.storm.generated.GlobalStreamId;
import backtype.storm.grouping.CustomStreamGrouping;
import backtype.storm.task.WorkerTopologyContext;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * NOTE: This is copy from storm 1.0.0 code. DON'T modify it.
 *
 * @since May 4, 2016
 */
public class ShuffleGrouping implements CustomStreamGrouping, Serializable {
    private static final long serialVersionUID = 5035497345182141765L;
    private Random random;
    private ArrayList<List<Integer>> choices;
    private AtomicInteger current;

    @Override
    public void prepare(WorkerTopologyContext context, GlobalStreamId stream, List<Integer> targetTasks) {
        random = new Random();
        choices = new ArrayList<List<Integer>>(targetTasks.size());
        for (Integer i : targetTasks) {
            choices.add(Arrays.asList(i));
        }
        Collections.shuffle(choices, random);
        current = new AtomicInteger(0);
    }

    @Override
    public List<Integer> chooseTasks(int taskId, List<Object> values) {
        int rightNow;
        int size = choices.size();
        while (true) {
            rightNow = current.incrementAndGet();
            if (rightNow < size) {
                return choices.get(rightNow);
            } else if (rightNow == size) {
                current.set(0);
                //This should be thread safe so long as ArrayList does not have any internal state that can be messed up by multi-treaded access.
                Collections.shuffle(choices, random);
                return choices.get(0);
            }
            //race condition with another thread, and we lost
            // try again
        }
    }

}
