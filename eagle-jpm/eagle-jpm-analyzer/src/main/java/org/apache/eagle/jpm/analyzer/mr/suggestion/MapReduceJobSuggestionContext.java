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
import org.apache.eagle.jpm.analyzer.meta.model.MapReduceAnalyzerEntity;
import org.apache.eagle.jpm.mr.historyentity.TaskAttemptExecutionAPIEntity;
import org.apache.eagle.jpm.util.MRJobTagName;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.TaskType;

import java.util.regex.Pattern;

import static org.apache.eagle.jpm.util.jobcounter.JobCounters.CounterName.MAP_OUTPUT_BYTES;
import static org.apache.eagle.jpm.util.jobcounter.JobCounters.CounterName.MAP_OUTPUT_RECORDS;
import static org.apache.hadoop.mapreduce.MRJobConfig.NUM_MAPS;
import static org.apache.hadoop.mapreduce.MRJobConfig.NUM_REDUCES;

public class MapReduceJobSuggestionContext {

    private JobConf jobconf;
    private MapReduceAnalyzerEntity job;

    private long numMaps;
    private long numReduces;

    private long avgMapTimeInSec;
    private long avgReduceTimeInSec;
    private long avgShuffleTimeInSec;
    private TaskAttemptExecutionAPIEntity worstMap;
    private TaskAttemptExecutionAPIEntity worstReduce;
    private TaskAttemptExecutionAPIEntity worstShuffle;
    private TaskAttemptExecutionAPIEntity lastMap;
    private TaskAttemptExecutionAPIEntity lastReduce;
    private TaskAttemptExecutionAPIEntity lastShuffle;
    private TaskAttemptExecutionAPIEntity firstMap;
    private TaskAttemptExecutionAPIEntity firstReduce;
    private TaskAttemptExecutionAPIEntity firstShuffle;

    private long minMapSpillMemBytes;

    public static final Pattern MAX_HEAP_PATTERN = Pattern.compile("-Xmx([0-9]+)([kKmMgG]?)");

    public MapReduceJobSuggestionContext(MapReduceAnalyzerEntity job) {
        this.job = job;
        this.jobconf = new JobConf(job.getJobConf());
        buildContext();
    }

    private MapReduceJobSuggestionContext buildContext() {
        avgMapTimeInSec = avgReduceTimeInSec = avgShuffleTimeInSec = 0;
        numMaps = jobconf.getLong(NUM_MAPS, 0);
        numReduces = jobconf.getLong(NUM_REDUCES, 0);

        for (TaskAttemptExecutionAPIEntity attempt : job.getCompletedTaskAttemptsMap().values()) {
            if (TaskType.MAP.toString().equalsIgnoreCase(getTaskType(attempt))) {
                long mapTime = attempt.getEndTime() - attempt.getStartTime();
                avgMapTimeInSec += mapTime;
                if (firstMap == null || firstMap.getStartTime() > attempt.getStartTime()) {
                    firstMap = attempt;
                }
                if (lastMap == null || lastMap.getEndTime() < attempt.getEndTime()) {
                    lastMap = attempt;
                }
                if (worstMap == null || (worstMap.getEndTime() - worstMap.getStartTime()) < mapTime) {
                    worstMap = attempt;
                }
                long tmpMem = getMinimumIOSortMemory(attempt);
                if (tmpMem > minMapSpillMemBytes) {
                    minMapSpillMemBytes = tmpMem;
                }
            } else if (TaskType.REDUCE.toString().equalsIgnoreCase(getTaskType(attempt))) {
                long shuffleTime = attempt.getShuffleFinishTime() - attempt.getStartTime();
                avgShuffleTimeInSec += shuffleTime;
                if (firstShuffle == null || firstShuffle.getStartTime() > attempt.getStartTime()) {
                    firstShuffle = attempt;
                }
                if (lastShuffle == null || lastShuffle.getShuffleFinishTime() < attempt.getShuffleFinishTime()) {
                    lastShuffle = attempt;
                }
                if (worstShuffle == null || (worstShuffle.getShuffleFinishTime() - worstShuffle.getStartTime()) < shuffleTime) {
                    worstShuffle = attempt;
                }

                long reduceTime = attempt.getEndTime() - attempt.getShuffleFinishTime();
                avgReduceTimeInSec += reduceTime;
                if (firstReduce == null || firstReduce.getStartTime() > attempt.getStartTime()) {
                    firstReduce = attempt;
                }
                if (lastReduce == null || lastReduce.getEndTime() < attempt.getEndTime()) {
                    lastReduce = attempt;
                }
                if (worstReduce == null || (worstReduce.getEndTime() - worstReduce.getShuffleFinishTime()) < reduceTime) {
                    worstReduce = attempt;
                }
            }
        }
        if (numMaps > 0) {
            avgMapTimeInSec = avgMapTimeInSec / numMaps / DateTimeUtil.ONESECOND;
        }
        if (numReduces > 0) {
            avgReduceTimeInSec = avgReduceTimeInSec / numReduces / DateTimeUtil.ONESECOND;
            avgShuffleTimeInSec = avgShuffleTimeInSec / numReduces / DateTimeUtil.ONESECOND;
        }
        return this;
    }

    private String getTaskType(TaskAttemptExecutionAPIEntity taskAttemptInfo) {
        return taskAttemptInfo.getTags().get(MRJobTagName.TASK_TYPE);
    }

    /**
     * The default index size is 16.
     *
     * @param attempt
     * @return minimal sort memory
     */
    private long getMinimumIOSortMemory(TaskAttemptExecutionAPIEntity attempt) {
        long records = attempt.getJobCounters().getCounterValue(MAP_OUTPUT_RECORDS);
        long outputBytes = attempt.getJobCounters().getCounterValue(MAP_OUTPUT_BYTES);
        return outputBytes + records * 16;
    }

    public JobConf getJobconf() {
        return jobconf;
    }

    public MapReduceAnalyzerEntity getJob() {
        return job;
    }

    public long getNumMaps() {
        return numMaps;
    }

    public long getNumReduces() {
        return numReduces;
    }

    public long getAvgMapTimeInSec() {
        return avgMapTimeInSec;
    }

    public long getAvgReduceTimeInSec() {
        return avgReduceTimeInSec;
    }

    public long getAvgShuffleTimeInSec() {
        return avgShuffleTimeInSec;
    }

    public TaskAttemptExecutionAPIEntity getWorstMap() {
        return worstMap;
    }

    public TaskAttemptExecutionAPIEntity getWorstReduce() {
        return worstReduce;
    }

    public TaskAttemptExecutionAPIEntity getWorstShuffle() {
        return worstShuffle;
    }

    public TaskAttemptExecutionAPIEntity getLastMap() {
        return lastMap;
    }

    public TaskAttemptExecutionAPIEntity getLastReduce() {
        return lastReduce;
    }

    public TaskAttemptExecutionAPIEntity getLastShuffle() {
        return lastShuffle;
    }

    public TaskAttemptExecutionAPIEntity getFirstMap() {
        return firstMap;
    }

    public TaskAttemptExecutionAPIEntity getFirstReduce() {
        return firstReduce;
    }

    public TaskAttemptExecutionAPIEntity getFirstShuffle() {
        return firstShuffle;
    }

    public long getMinMapSpillMemBytes() {
        return minMapSpillMemBytes;
    }
}
