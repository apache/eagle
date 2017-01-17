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

import org.apache.eagle.jpm.analyzer.Processor;
import org.apache.eagle.jpm.analyzer.meta.model.MapReduceAnalyzerEntity;
import org.apache.eagle.jpm.analyzer.publisher.Result;
import org.apache.eagle.jpm.mr.historyentity.TaskAttemptExecutionAPIEntity;
import org.apache.eagle.jpm.util.jobcounter.JobCounters;

public class MapReduceDataSkewAnalyzer implements Processor<MapReduceAnalyzerEntity> {
    private MapReduceJobSuggestionContext context;
    public MapReduceDataSkewAnalyzer(MapReduceJobSuggestionContext context) {
        this.context = context;
    }
    @Override
    public Result.ProcessorResult process(MapReduceAnalyzerEntity jobAnalysisEntity) {
        TaskAttemptExecutionAPIEntity worstReduce = context.getWorstReduce();
        if (context.getNumReduces() == 0 || worstReduce == null) {
            return null;
        }
        StringBuilder sb = new StringBuilder();
        try {
            long worstTime = (worstReduce.getEndTime() - worstReduce
                .getShuffleFinishTime()) / 1000;
            if (worstTime - context.getAvgReduceTimeInSec() > 30 * 60 ) {
                long avgInputs = context.getJob().getReduceCounters().getCounterValue(JobCounters.CounterName.REDUCE_INPUT_RECORDS)
                    / context.getNumReduces();
                long worstInputs = worstReduce.getJobCounters().getCounterValue(JobCounters.CounterName.REDUCE_INPUT_RECORDS);

                if (worstInputs / 5 > avgInputs) {
                    sb.append("Data skew detected in reducers. The average reduce time is "
                        + context.getAvgReduceTimeInSec());
                    sb.append(" seconds, the worst reduce time is " + worstTime);
                    sb.append(" seconds. Please investigate this problem to improve your job performance.\n");
                }
            }

            if (sb.length() > 0) {
                return new Result.ProcessorResult(Result.ResultLevel.WARNING, sb.toString());
            }
        } catch (NullPointerException e) {
            // When job failed there may not have counters, so just ignore it
        }
        return null;
    }
}
