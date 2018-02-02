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

package org.apache.eagle.jpm.mr.history.publisher;

import org.apache.eagle.dataproc.impl.storm.ValuesArray;
import org.apache.eagle.jpm.mr.historyentity.JobRpcAnalysisAPIEntity;
import org.apache.eagle.jpm.util.MRJobTagName;

import java.util.HashMap;
import java.util.Map;


public class JobRpcAnalysisStreamPublisher extends StreamPublisher<JobRpcAnalysisAPIEntity> {

    public JobRpcAnalysisStreamPublisher(String stormStreamId) {
        super(stormStreamId);
    }

    @Override
    public Class<?> type() {
        return JobRpcAnalysisAPIEntity.class;
    }

    @Override
    public void flush(JobRpcAnalysisAPIEntity entity) {
        Map<String, Object> fields = new HashMap<>(entity.getTags());
        fields.put("trackingUrl", entity.getTrackingUrl());
        fields.put("totalOpsPerSecond", entity.getTotalOpsPerSecond());
        fields.put("mapOpsPerSecond", entity.getMapOpsPerSecond());
        fields.put("reduceOpsPerSecond", entity.getReduceOpsPerSecond());
        fields.put("avgOpsPerTask", entity.getAvgOpsPerTask());
        fields.put("avgOpsPerMap", entity.getAvgOpsPerMap());
        fields.put("avgOpsPerReduce", entity.getAvgOpsPerReduce());
        fields.put("currentState", entity.getCurrentState());
        fields.put("numTotalMaps", entity.getNumTotalMaps());
        fields.put("numTotalReduces", entity.getNumTotalReduces());
        fields.put("duration", entity.getDuration());
        fields.put("avgMapTime", entity.getAvgMapTime());
        fields.put("avgReduceTime", entity.getAvgReduceTime());

        collector.collect(stormStreamId, new ValuesArray(fields.get(MRJobTagName.JOB_ID.toString()), fields));
    }


}
