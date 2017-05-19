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

package org.apache.eagle.jpm.analyzer.mr.rpc;

import org.apache.eagle.jpm.analyzer.Evaluator;
import org.apache.eagle.jpm.analyzer.meta.model.MapReduceAnalyzerEntity;
import org.apache.eagle.jpm.analyzer.publisher.Result;
import org.apache.eagle.jpm.mr.historyentity.JobRpcAnalysisAPIEntity;
import org.apache.eagle.jpm.mr.historyentity.TaskExecutionAPIEntity;
import org.apache.eagle.jpm.util.Constants;
import org.apache.eagle.jpm.util.jobcounter.JobCounters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import static org.apache.eagle.jpm.util.MRJobTagName.*;

public class JobRpcEvaluator implements Evaluator<MapReduceAnalyzerEntity>, Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(JobRpcEvaluator.class);

    @Override
    public Result.EvaluatorResult evaluate(MapReduceAnalyzerEntity entity) {
        try {
            double totalMapHdfsOps = 0;
            double totalReduceHdfsOps = 0;

            if (entity.getFinishedMaps() != 0) {
                totalMapHdfsOps = getTotalHdfsOps(entity.getMapCounters());
            }
            if (entity.getFinishedReduces() != 0) {
                totalReduceHdfsOps = getTotalHdfsOps(entity.getReduceCounters());
            }

            long mapStartTime = Long.MAX_VALUE;
            long mapEndTime = 0;
            long reduceStartTime = Long.MAX_VALUE;
            long reduceEndTime = 0;

            for (TaskExecutionAPIEntity task : entity.getTasksMap().values()) {
                if (task.getTags().get(TASK_TYPE.toString()).equalsIgnoreCase(Constants.TaskType.MAP.toString())) {
                    if (mapStartTime > task.getStartTime()) {
                        mapStartTime = task.getStartTime();
                    }
                    if (mapEndTime < task.getEndTime()) {
                        mapEndTime = task.getEndTime();
                    }
                } else {
                    if (reduceStartTime > task.getStartTime()) {
                        reduceStartTime = task.getStartTime();
                    }
                    if (reduceEndTime < task.getEndTime()) {
                        reduceEndTime = task.getEndTime();
                    }
                }
            }

            Map<String, String> tags = new HashMap<>();
            tags.put(SITE.toString(), entity.getSiteId());
            tags.put(USER.toString(), entity.getUserId());
            tags.put(JOB_QUEUE.toString(), entity.getJobQueueName());
            tags.put(JOD_DEF_ID.toString(), entity.getJobDefId());
            tags.put(JOB_TYPE.toString(), entity.getJobType());
            tags.put(JOB_ID.toString(), entity.getJobId());
            JobRpcAnalysisAPIEntity analysisAPIEntity = new JobRpcAnalysisAPIEntity();
            analysisAPIEntity.setTags(tags);
            analysisAPIEntity.setTimestamp(entity.getStartTime());
            analysisAPIEntity.setTrackingUrl(entity.getTrackingUrl());

            double totalOpsPerSecond = (entity.getDurationTime() == 0) ? 0 :
                    (totalMapHdfsOps + totalReduceHdfsOps) / (entity.getDurationTime() / 1000);
            double mapOpsPerSecond = (entity.getTotalMaps() == 0) ? 0 :
                    totalMapHdfsOps / ((mapEndTime - mapStartTime) / 1000);
            double reduceOpsPerSecond = (entity.getTotalReduces() == 0) ? 0 :
                    totalReduceHdfsOps / ((reduceEndTime - reduceStartTime) / 1000);
            
            double avgOpsPerTask = (totalMapHdfsOps + totalReduceHdfsOps) / (entity.getTotalMaps() + entity.getTotalReduces());
            double avgOpsPerMap = (entity.getTotalMaps() == 0) ? 0 :
                    totalMapHdfsOps / entity.getTotalMaps();
            double avgOpsPerReduce = (entity.getTotalReduces() == 0) ? 0 :
                    totalReduceHdfsOps / entity.getTotalReduces();

            analysisAPIEntity.setTotalOpsPerSecond(totalOpsPerSecond);
            analysisAPIEntity.setMapOpsPerSecond(mapOpsPerSecond);
            analysisAPIEntity.setReduceOpsPerSecond(reduceOpsPerSecond);
            analysisAPIEntity.setAvgOpsPerTask(avgOpsPerTask);
            analysisAPIEntity.setAvgOpsPerMap(avgOpsPerMap);
            analysisAPIEntity.setAvgOpsPerReduce(avgOpsPerReduce);

            Result.EvaluatorResult result = new Result.EvaluatorResult();
            result.addProcessorEntity(JobRpcEvaluator.class, analysisAPIEntity);

            return result;
        } catch (Exception e) {
            LOG.error("Rpc analysis failed for job {} due to {}", entity.getJobId(), e.getMessage());
            return null;
        }

    }

    private long getTotalHdfsOps(JobCounters counter) {
        long mapHdfsReadOps = counter.getCounterValue(JobCounters.CounterName.HDFS_READ_OPS);
        long mapHdfsWriteOps = counter.getCounterValue(JobCounters.CounterName.HDFS_WRITE_OPS);
        return  mapHdfsReadOps + mapHdfsWriteOps;
    }

}
