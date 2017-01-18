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
import org.apache.eagle.jpm.util.jobcounter.JobCounters;

import static org.apache.hadoop.mapreduce.MRJobConfig.MAP_JAVA_OPTS;
import static org.apache.hadoop.mapreduce.MRJobConfig.REDUCE_JAVA_OPTS;

public class MapReduceGCTimeProcessor implements Processor<MapReduceAnalyzerEntity> {
    private MapReduceJobSuggestionContext context;

    public MapReduceGCTimeProcessor(MapReduceJobSuggestionContext context) {
        this.context = context;
    }

    @Override
    public Result.ProcessorResult process(MapReduceAnalyzerEntity jobAnalysisEntity) {
        StringBuilder sb = new StringBuilder();
        try {
            long mapGCTime = context.getJob().getMapCounters().getCounterValue(JobCounters.CounterName.GC_MILLISECONDS);
            long mapCPUTime = context.getJob().getMapCounters().getCounterValue(JobCounters.CounterName.CPU_MILLISECONDS);

            if (mapGCTime > mapCPUTime * 0.1) {
                sb.append("Map GC_TIME_MILLIS took too long. Please increase mapper memory via -D"
                    + MAP_JAVA_OPTS);
                sb.append(", or optimize your mapper class.\n");
            }

            if (context.getNumReduces() > 0) {
                long reduceGCTime = context.getJob().getReduceCounters().getCounterValue(JobCounters.CounterName.GC_MILLISECONDS);
                long reduceCPUTime = context.getJob().getReduceCounters().getCounterValue(JobCounters.CounterName.CPU_MILLISECONDS);
                if (reduceGCTime > reduceCPUTime * 0.1) {
                    sb.append("Reduce GC_TIME_MILLIS took too long. Please increase memory for reduce via -D"
                        + REDUCE_JAVA_OPTS);
                    sb.append(", or optimize your reducer class.\n");
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
