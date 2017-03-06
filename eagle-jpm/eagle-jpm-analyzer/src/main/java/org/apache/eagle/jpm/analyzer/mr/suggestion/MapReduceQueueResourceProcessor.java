/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.eagle.jpm.analyzer.mr.suggestion;

import org.apache.eagle.common.DateTimeUtil;
import org.apache.eagle.jpm.analyzer.Processor;
import org.apache.eagle.jpm.analyzer.meta.model.MapReduceAnalyzerEntity;
import org.apache.eagle.jpm.analyzer.publisher.Result;
import org.apache.eagle.jpm.mr.historyentity.TaskAttemptExecutionAPIEntity;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * Criterion: (TimeElapsed / (numTasks / 500 * avgTaskTime)) > 20
 */
public class MapReduceQueueResourceProcessor implements Processor<MapReduceAnalyzerEntity> {
    private static final Logger LOG = LoggerFactory.getLogger(MapReduceQueueResourceProcessor.class);

    private MapReduceJobSuggestionContext context;

    public MapReduceQueueResourceProcessor(MapReduceJobSuggestionContext context) {
        this.context = context;
    }

    @Override
    public Result.ProcessorResult process(MapReduceAnalyzerEntity jobAnalysisEntity) {
        try {
            String userName = context.getJob().getUserId();
            TaskAttemptExecutionAPIEntity lastMap = context.getLastMap();
            TaskAttemptExecutionAPIEntity firstMap = context.getFirstMap();
            TaskAttemptExecutionAPIEntity lastReduce = context.getLastReduce();
            TaskAttemptExecutionAPIEntity firstShuffle = context.getFirstShuffle();

            if (checkBatchUser(userName) && lastMap != null && firstMap != null) {
                StringBuilder sb = new StringBuilder();

                long tasksPerTime = 500; // better get it from RM
                long mapPhaseTimeInSec = (lastMap.getEndTime() - firstMap.getStartTime()) / DateTimeUtil.ONESECOND;
                if (mapPhaseTimeInSec > context.getAvgMapTimeInSec()
                    * ((context.getNumMaps() + tasksPerTime - 1) / tasksPerTime) * 20) {
                    sb.append("There appears to have been resource contention during the map phase of your job. Please ask for more resources if your job is SLA-bound,");
                    sb.append(" or submit your job when the cluster is less busy.\n");
                }

                if (context.getNumReduces() > 0 && lastReduce != null && firstShuffle != null) {
                    long reducePhaseTimeInSec = (lastReduce.getEndTime() - firstShuffle.getStartTime()) / DateTimeUtil.ONESECOND;
                    if (reducePhaseTimeInSec > context.getAvgReduceTimeInSec()
                        * ((context.getNumReduces() + tasksPerTime - 1) / tasksPerTime) * 20) {
                        sb.append("Seems there was resource contention when your job in reduce phase, please ask for more resource if your job is SLA enabled,");
                        sb.append(" or submit your job when the cluster is less busy.\n");
                    }
                }

                if (sb.length() > 0) {
                    return new Result.ProcessorResult(Result.RuleType.RESOURCE_CONTENTION, Result.ResultLevel.INFO, sb.toString());
                }
            }
        } catch (Exception e) {
            LOG.warn(e.getMessage(), e);
        }
        return null;
    }

    protected boolean checkBatchUser(String userName) {
        return userName.startsWith("b_");
    }
}