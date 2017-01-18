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

public class MapReduceTaskNumAnalyzer implements Processor<MapReduceAnalyzerEntity> {

    private MapReduceJobSuggestionContext context;

    public MapReduceTaskNumAnalyzer(MapReduceJobSuggestionContext context) {
        this.context = context;
    }

    @Override
    public Result.ProcessorResult process(MapReduceAnalyzerEntity jobAnalysisEntity) {
        StringBuilder sb = new StringBuilder();
        try {
            long numReduces = context.getNumReduces();
            long numMaps = context.getNumMaps();
            if (numReduces > 0) {
                long avgReduceTime = context.getAvgReduceTimeInSec();
                long avgShuffleTime = context.getAvgShuffleTimeInSec();
                long avgShuffleBytes = context.getJob().getReduceCounters().getCounterValue(JobCounters.CounterName.REDUCE_SHUFFLE_BYTES)
                    / numReduces;
                long avgReduceOutput = context.getJob().getReduceCounters().getCounterValue(JobCounters.CounterName.HDFS_BYTES_WRITTEN)
                    / numReduces;
                long avgReduceTotalTime = avgShuffleTime + avgReduceTime;

                long suggestReduces = 0;
                StringBuilder tmpsb = new StringBuilder();

                if (avgShuffleBytes < 256 * FileUtils.ONE_MB && avgReduceTotalTime < 300
                    && avgReduceOutput < 256 * FileUtils.ONE_MB && numReduces > 1) {
                    tmpsb.append("average reduce input bytes is: ");
                    tmpsb.append(FileUtils.byteCountToDisplaySize(avgShuffleBytes) + ", ");
                    if (suggestReduces == 0) {
                        suggestReduces = getReduceNum(avgShuffleBytes, avgReduceOutput, avgReduceTime);
                    }
                } else if (avgShuffleBytes > 10 * FileUtils.ONE_GB && avgReduceTotalTime > 1800) {
                    tmpsb.append("average reduce input bytes is: ");
                    tmpsb.append(FileUtils.byteCountToDisplaySize(avgShuffleBytes) + ", ");
                    if (suggestReduces == 0) {
                        suggestReduces = getReduceNum(avgShuffleBytes, avgReduceOutput, avgReduceTime);
                    }
                }

                if (avgReduceTotalTime < 60 && numReduces > 1) {
                    tmpsb.append("average reduce time is only "
                        + (context.getAvgShuffleTimeInSec() + context.getAvgReduceTimeInSec()) + " seconds, ");
                    if (suggestReduces == 0) {
                        suggestReduces = getReduceNum(avgShuffleBytes, avgReduceOutput, avgReduceTime);
                    }
                } else if (avgReduceTotalTime > 3600 && avgReduceTime > 1800) {
                    tmpsb.append("average reduce time is "
                        + (avgShuffleTime + avgReduceTime) + " seconds, ");
                    if (suggestReduces == 0) {
                        suggestReduces = getReduceNum(avgShuffleBytes, avgReduceOutput, avgReduceTime);
                    }
                }

                if (avgReduceOutput < 10 * FileUtils.ONE_MB && avgReduceTime < 300
                    && avgShuffleBytes < 2 * FileUtils.ONE_GB && numReduces > 1) {
                    tmpsb.append(" average reduce output is only ");
                    tmpsb.append(FileUtils.byteCountToDisplaySize(avgReduceOutput) + ", ");
                    if (suggestReduces == 0) {
                        suggestReduces = getReduceNum(avgShuffleBytes, avgReduceOutput, avgReduceTime);
                    }
                } else if (avgReduceOutput > 10 * FileUtils.ONE_GB && avgReduceTime > 1800) {
                    tmpsb.append(" average reduce output is ");
                    tmpsb.append(FileUtils.byteCountToDisplaySize(avgReduceOutput) + ", ");
                    if (suggestReduces == 0) {
                        suggestReduces = getReduceNum(avgShuffleBytes, avgReduceOutput, avgReduceTime);
                    }
                }

                if (suggestReduces > 0) {
                    sb.append("Best practice: ");
                    sb.append(tmpsb.toString());
                    sb.append("please consider ");
                    if (suggestReduces > numReduces) {
                        sb.append("increasing the ");
                    } else {
                        sb.append("decreasing the ");
                    }
                    sb.append("reducer number. You could try -Dmapreduce.job.reduces=" + suggestReduces + "\n");
                }
            }

            // map numbers
            long avgMapInput = context.getJob().getMapCounters().getCounterValue(JobCounters.CounterName.HDFS_BYTES_READ)
                / numMaps;
            long avgMapTime = context.getAvgMapTimeInSec();
            if (avgMapInput < 5 * FileUtils.ONE_MB && avgMapTime < 30 && numMaps > 1) {
                sb.append("Best practice: average map input bytes only have ");
                sb.append(FileUtils.byteCountToDisplaySize(avgMapInput));
                sb.append(". Please reduce the number of mappers by merging input files.\n");
            } else if (avgMapInput > FileUtils.ONE_GB) {
                sb.append("Best practice: average map input bytes have ");
                sb.append(FileUtils.byteCountToDisplaySize(avgMapInput));
                sb.append(". Please increase the number of mappers by using splittable compression, a container file format or a smaller block size.\n");
            }

            if (avgMapTime < 10 && numMaps > 1) {
                sb.append("Best practice: average map time only have " + avgMapTime);
                sb.append(" seconds. Please reduce the number of mappers by merging input files or by using a larger block size.\n");
            } else if (avgMapTime > 600 && avgMapInput < FileUtils.ONE_GB) {
                sb.append("Best practice: average map time is " + avgMapInput);
                sb.append(" seconds. Please increase the number of mappers by using splittable compression, a container file format or a smaller block size.\n");
            }

            if (sb.length() > 0) {
                return new Result.ProcessorResult(Result.ResultLevel.CRITICAL, sb.toString());
            }
        } catch (NullPointerException e) {
            // When job failed there may not have counters, so just ignore it
        }
        return null;
    }

    private long getReduceNum(long avgInputBytes, long avgOutputBytes, long avgTime) {
        long newReduceNum = 1;
        long tmpReduceNum;

        long numReduces = context.getNumReduces();
        tmpReduceNum = avgInputBytes * numReduces / (3 * FileUtils.ONE_GB);
        if (tmpReduceNum > newReduceNum) {
            newReduceNum = tmpReduceNum;
        }

        tmpReduceNum = avgOutputBytes * numReduces / (2 * FileUtils.ONE_GB);
        if (tmpReduceNum > newReduceNum) {
            newReduceNum = tmpReduceNum;
        }

        tmpReduceNum = avgTime * numReduces / (10 * 60);
        if (tmpReduceNum > newReduceNum) {
            newReduceNum = tmpReduceNum;
        }

        return newReduceNum;
    }

}