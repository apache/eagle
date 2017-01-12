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

package org.apache.eagle.jpm.analyzer.mr.sla;

import com.typesafe.config.Config;
import org.apache.eagle.jpm.analyzer.AnalyzerEntity;
import org.apache.eagle.jpm.analyzer.Evaluator;
import org.apache.eagle.jpm.analyzer.meta.model.JobMetaEntity;
import org.apache.eagle.jpm.analyzer.mr.sla.processors.LongStuckJobProcessor;
import org.apache.eagle.jpm.analyzer.mr.sla.processors.UnExpectedLongDurationJobProcessor;
import org.apache.eagle.jpm.analyzer.Processor;
import org.apache.eagle.jpm.analyzer.publisher.Result;
import org.apache.eagle.jpm.analyzer.util.Utils;
import org.apache.eagle.jpm.util.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class SLAJobEvaluator implements Evaluator, Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(SLAJobEvaluator.class);

    private List<Processor> processors = new ArrayList<>();
    private Config config;

    public SLAJobEvaluator(Config config) {
        this.config = config;
        processors.add(new UnExpectedLongDurationJobProcessor(config));
        processors.add(new LongStuckJobProcessor(config));
    }

    @Override
    public Result.EvaluatorResult evaluate(AnalyzerEntity analyzerJobEntity) {
        if (!analyzerJobEntity.getCurrentState().equalsIgnoreCase(Constants.JobState.RUNNING.toString())) {
            return null;
        }

        Result.EvaluatorResult result = new Result.EvaluatorResult();

        List<JobMetaEntity> jobMetaEntities = Utils.getJobMeta(config, analyzerJobEntity.getJobDefId());
        if (jobMetaEntities.size() == 0
                || !jobMetaEntities.get(0).getEvaluators().contains(this.getClass().getName())) {
            LOG.info("SLAJobEvaluator skip job {}", analyzerJobEntity.getJobDefId());
            return result;
        }

        analyzerJobEntity.setJobMeta(jobMetaEntities.get(0).getConfiguration());

        for (Processor processor : processors) {
            result.addProcessorResult(processor.getClass(), processor.process(analyzerJobEntity));
        }

        return result;
    }
}
