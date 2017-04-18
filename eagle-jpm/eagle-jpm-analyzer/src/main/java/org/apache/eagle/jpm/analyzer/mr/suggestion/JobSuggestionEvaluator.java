/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.eagle.jpm.analyzer.mr.suggestion;

import com.typesafe.config.Config;
import org.apache.eagle.jpm.analyzer.Evaluator;
import org.apache.eagle.jpm.analyzer.Processor;
import org.apache.eagle.jpm.mr.historyentity.JobSuggestionAPIEntity;
import org.apache.eagle.jpm.analyzer.meta.model.MapReduceAnalyzerEntity;
import org.apache.eagle.jpm.analyzer.publisher.Result;
import org.apache.eagle.jpm.util.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.eagle.jpm.util.MRJobTagName.*;

public class JobSuggestionEvaluator implements Evaluator<MapReduceAnalyzerEntity>, Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(JobSuggestionEvaluator.class);

    private Config config;

    public JobSuggestionEvaluator(Config config) {
        this.config = config;
    }

    private List<Processor> loadProcessors(MapReduceJobSuggestionContext context) {
        List<Processor> processors = new ArrayList<>();
        processors.add(new MapReduceCompressionSettingProcessor(context));
        processors.add(new MapReduceSplitSettingProcessor(context));
        processors.add(new MapReduceDataSkewProcessor(context));
        processors.add(new MapReduceGCTimeProcessor(context));
        processors.add(new MapReduceSpillProcessor(context));
        processors.add(new MapReduceTaskNumProcessor(context));
        //processors.add(new MapReduceQueueResourceProcessor(context));

        return processors;
    }

    @Override
    public Result.EvaluatorResult evaluate(MapReduceAnalyzerEntity analyzerEntity) {
        if (analyzerEntity.getCurrentState().equalsIgnoreCase(Constants.JobState.RUNNING.toString())) {
            return null;
        }


        if (analyzerEntity.getTotalCounters() == null) {
            LOG.warn("Total counters of Job {} is null", analyzerEntity.getJobId());
            return null;
        }
        if (analyzerEntity.getMapCounters() == null && analyzerEntity.getReduceCounters() == null) {
            LOG.warn("Map/Reduce task counters of Job {} are null", analyzerEntity.getJobId());
            return null;
        }

        try {
            Result.EvaluatorResult result = new Result.EvaluatorResult();

            MapReduceJobSuggestionContext jobContext = new MapReduceJobSuggestionContext(analyzerEntity);
            if (jobContext.getNumMaps() == 0) {
                return null;
            }

            for (Processor processor : loadProcessors(jobContext)) {
                Result.ProcessorResult processorResult = processor.process(analyzerEntity);
                if (processorResult != null) {
                    result.addProcessorResult(processor.getClass(), processorResult);
                    result.addProcessorEntity(processor.getClass(), createJobSuggestionEntity(processorResult, analyzerEntity));
                }
            }
            return result;
        } catch (Exception ex) {
            LOG.error(ex.getMessage(), ex);
            return null;
        }

    }

    private static JobSuggestionAPIEntity createJobSuggestionEntity(Result.ProcessorResult processorResult, MapReduceAnalyzerEntity entity) {
        Map<String, String> tags = new HashMap<>();
        tags.put(JOB_ID.toString(), entity.getJobId());
        tags.put(JOD_DEF_ID.toString(), entity.getJobDefId());
        tags.put(SITE.toString(), entity.getSiteId());
        tags.put(USER.toString(), entity.getUserId());
        tags.put(RULE_TYPE.toString(), processorResult.getRuleType().toString());
        tags.put(JOB_QUEUE.toString(), entity.getJobQueueName());
        tags.put(JOB_TYPE.toString(), entity.getJobType());
        JobSuggestionAPIEntity jobSuggestionAPIEntity = new JobSuggestionAPIEntity();
        jobSuggestionAPIEntity.setTags(tags);
        jobSuggestionAPIEntity.setTimestamp(entity.getStartTime());  // startTime as the job timestamp
        jobSuggestionAPIEntity.setOptimizerSuggestion(processorResult.getMessage());
        jobSuggestionAPIEntity.setOptimizerSettings(processorResult.getSettings());

        return jobSuggestionAPIEntity;
    }

}
