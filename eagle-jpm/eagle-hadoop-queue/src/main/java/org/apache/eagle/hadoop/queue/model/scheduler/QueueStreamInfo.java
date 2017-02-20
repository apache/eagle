/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.eagle.hadoop.queue.model.scheduler;

import org.apache.eagle.hadoop.queue.common.HadoopClusterConstants;

import java.util.HashMap;
import java.util.Map;

public class QueueStreamInfo {
    private static final String TIMESTAMP = "timestamp";
    private static final String QUEUE_SITE = "site";
    public static final String QUEUE_NAME = "queue";
    private static final String QUEUE_STATE = "state";
    private static final String QUEUE_SCHEDULER = "scheduler";
    private static final String QUEUE_ABSOLUTE_CAPACITY = "absoluteCapacity";
    private static final String QUEUE_ABSOLUTE_MAX_CAPACITY = "absoluteMaxCapacity";
    private static final String QUEUE_ABSOLUTE_USED_CAPACITY = "absoluteUsedCapacity";
    private static final String QUEUE_MAX_USER_USED_CAPACITY = "maxUserUsedCapacity";
    private static final String QUEUE_USER_LIMIT_CAPACITY = "userLimitCapacity";
    private static final String QUEUE_USED_MEMORY = "memory";
    private static final String QUEUE_USED_VCORES = "vcores";
    private static final String QUEUE_NUM_ACTIVE_APPS = "numActiveApplications";
    private static final String QUEUE_NUM_PENDING_APPS = "numPendingApplications";
    private static final String QUEUE_MAX_ACTIVE_APPS = "maxActiveApplications";


    public static Map<String, Object> convertEntityToStream(RunningQueueAPIEntity queueAPIEntity) {
        Map<String, Object> queueInfoMap = new HashMap<>();
        queueInfoMap.put(QueueStreamInfo.QUEUE_SITE, queueAPIEntity.getTags().get(HadoopClusterConstants.TAG_SITE));
        queueInfoMap.put(QueueStreamInfo.QUEUE_NAME, queueAPIEntity.getTags().get(HadoopClusterConstants.TAG_QUEUE));
        queueInfoMap.put(QueueStreamInfo.QUEUE_ABSOLUTE_CAPACITY, queueAPIEntity.getAbsoluteCapacity());
        queueInfoMap.put(QueueStreamInfo.QUEUE_ABSOLUTE_MAX_CAPACITY, queueAPIEntity.getAbsoluteMaxCapacity());
        queueInfoMap.put(QueueStreamInfo.QUEUE_ABSOLUTE_USED_CAPACITY, queueAPIEntity.getAbsoluteUsedCapacity());
        queueInfoMap.put(QueueStreamInfo.QUEUE_MAX_ACTIVE_APPS, queueAPIEntity.getMaxActiveApplications());
        queueInfoMap.put(QueueStreamInfo.QUEUE_NUM_ACTIVE_APPS, queueAPIEntity.getNumActiveApplications());
        queueInfoMap.put(QueueStreamInfo.QUEUE_NUM_PENDING_APPS, queueAPIEntity.getNumPendingApplications());
        queueInfoMap.put(QueueStreamInfo.QUEUE_SCHEDULER, queueAPIEntity.getScheduler());
        queueInfoMap.put(QueueStreamInfo.QUEUE_STATE, queueAPIEntity.getState());
        queueInfoMap.put(QueueStreamInfo.QUEUE_USED_MEMORY, queueAPIEntity.getMemory());
        queueInfoMap.put(QueueStreamInfo.QUEUE_USED_VCORES, queueAPIEntity.getVcores());
        queueInfoMap.put(QueueStreamInfo.TIMESTAMP, queueAPIEntity.getTimestamp());

        double maxUserUsedCapacity = 0;
        double userUsedCapacity;
        for (UserWrapper user : queueAPIEntity.getUsers().getUsers()) {
            userUsedCapacity = calculateUserUsedCapacity(
                    queueAPIEntity.getAbsoluteUsedCapacity(),
                    queueAPIEntity.getMemory(),
                    user.getMemory());
            if (userUsedCapacity > maxUserUsedCapacity) {
                maxUserUsedCapacity = userUsedCapacity;
            }

        }
        queueInfoMap.put(QueueStreamInfo.QUEUE_MAX_USER_USED_CAPACITY, maxUserUsedCapacity);
        queueInfoMap.put(QueueStreamInfo.QUEUE_USER_LIMIT_CAPACITY, queueAPIEntity.getUserLimitFactor() * queueAPIEntity.getAbsoluteCapacity());
        return queueInfoMap;
    }

    private static double calculateUserUsedCapacity(double absoluteUsedCapacity, long queueUsedMem, long userUsedMem) {
        return userUsedMem * absoluteUsedCapacity / queueUsedMem;
    }
}
