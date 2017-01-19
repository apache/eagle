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

import org.apache.commons.io.FileUtils;
import org.apache.eagle.jpm.analyzer.Processor;
import org.apache.eagle.jpm.analyzer.meta.model.MapReduceAnalyzerEntity;
import org.apache.eagle.jpm.analyzer.publisher.Result;
import org.apache.eagle.jpm.util.jobcounter.JobCounters;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;

import static org.apache.eagle.jpm.analyzer.mr.suggestion.MapReduceJobSuggestionContext.MAX_HEAP_PATTERN;
import static org.apache.hadoop.mapreduce.MRJobConfig.IO_SORT_MB;
import static org.apache.hadoop.mapreduce.MRJobConfig.MAP_JAVA_OPTS;
import static org.apache.hadoop.mapreduce.MRJobConfig.MAP_SORT_SPILL_PERCENT;

/**
 * Check whether spilled more than once, if true, find out the minimum value of the memory to hold all the data,
 * based on that value, find out how much memory need for heap size.
 */
public class MapReduceSpillProcessor implements Processor<MapReduceAnalyzerEntity> {

    private MapReduceJobSuggestionContext context;

    public MapReduceSpillProcessor(MapReduceJobSuggestionContext context) {
        this.context = context;
    }

    @Override
    public Result.ProcessorResult process(MapReduceAnalyzerEntity jobAnalysisEntity) {
        StringBuilder sb = new StringBuilder();
        List<String> optSettings = new ArrayList<>();
        String setting;

        long outputRecords = 0L; // Map output records
        long spillRecords = 0L; //  Spilled Records
        try {
            outputRecords = context.getJob().getMapCounters().getCounterValue(JobCounters.CounterName.MAP_OUTPUT_RECORDS);
            spillRecords = context.getJob().getMapCounters().getCounterValue(JobCounters.CounterName.SPILLED_RECORDS);

            if (outputRecords < spillRecords) {
                sb.append("Total Map output records: ").append(outputRecords);
                sb.append(" Total Spilled Records: ").append(spillRecords).append(". Please set");

                long minMapSpillMemBytes = context.getMinMapSpillMemBytes();
                double spillPercent = context.getJobconf().getDouble(MAP_SORT_SPILL_PERCENT, 0.8);
                if (minMapSpillMemBytes > 512 * FileUtils.ONE_MB * spillPercent) {
                    if (Math.abs(1.0 - spillPercent) > 0.001) {
                        setting = String.format("-D%s=1", MAP_SORT_SPILL_PERCENT);
                        sb.append(" ").append(setting);
                        optSettings.add(setting);
                    }
                } else {
                    minMapSpillMemBytes /= spillPercent;
                }

                long minMapSpillMemMB = (minMapSpillMemBytes / FileUtils.ONE_MB + 10) / 10 * 10;
                if (minMapSpillMemMB >= 2047 ) {
                    sb.append(" Please reduce the block size of the input files and make sure they are splittable.");
                } else {
                    setting = String.format("-D%s=%s", IO_SORT_MB, minMapSpillMemMB);
                    sb.append(" ").append(setting);
                    optSettings.add(setting);
                    long heapSize = getMaxHeapSize(context.getJobconf().get(MAP_JAVA_OPTS));
                    if (heapSize < 3 * minMapSpillMemMB) {
                        long expectedHeapSizeMB = (minMapSpillMemMB * 3 + 1024) / 1024 * 1024;
                        setting = String.format(" -D%s=-Xmx%sM", MAP_JAVA_OPTS, expectedHeapSizeMB);
                        sb.append(" ").append(setting);
                        optSettings.add(setting);
                    }
                }
                sb.append(" to avoid spilled records.\n");
            }


            long reduceInputRecords = context.getJob().getReduceCounters().getCounterValue(JobCounters.CounterName.REDUCE_INPUT_RECORDS);
            spillRecords = context.getJob().getReduceCounters().getCounterValue(JobCounters.CounterName.SPILLED_RECORDS);
            if (reduceInputRecords < spillRecords) {
                sb.append("Please add more memory (mapreduce.reduce.java.opts) to avoid spilled records.");
                sb.append(" Total Reduce input records: ").append(reduceInputRecords);
                sb.append(" Total Spilled Records: ").append(spillRecords);
                sb.append("\n");
            }

            if (sb.length() > 0) {
                return new Result.ProcessorResult(Result.RuleType.SPILL, Result.ResultLevel.INFO, sb.toString(), optSettings);
            }
        } catch (NullPointerException e) {
            //When job failed there may not have counters, so just ignore it
        }
        return null;
    }

    private static long getMaxHeapSize(String s) {
        Matcher m = MAX_HEAP_PATTERN.matcher(s);
        long val = 0;
        if (m.find()) {
            val = Long.parseLong(m.group(1));
            if ("k".equalsIgnoreCase(m.group(2))) {
                val /= 1024;
            } else if ("g".equalsIgnoreCase(m.group(2))) {
                val *= 1024;
            }
        }
        return val;
    }
}
