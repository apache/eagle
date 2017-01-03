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

package org.apache.eagle.jpm.analysis.mr;

import com.typesafe.config.Config;
import org.apache.eagle.jpm.analysis.*;
import org.apache.eagle.jpm.analysis.Evaluator;
import org.apache.eagle.jpm.analysis.mr.meta.MetaManagementService;
import org.apache.eagle.jpm.analysis.mr.meta.model.JobMetaEntity;
import org.apache.eagle.jpm.analysis.mr.sla.SLAJobEvaluator;
import org.apache.eagle.jpm.analysis.mr.suggestion.JobSuggestionEvaluator;
import org.apache.eagle.jpm.analysis.publisher.EagleStorePublisher;
import org.apache.eagle.jpm.analysis.publisher.EmailPublisher;
import org.apache.eagle.jpm.analysis.publisher.Publisher;
import org.apache.eagle.jpm.analysis.publisher.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.List;

public class MRJobPerformanceAnalyzer implements JobAnalyzer<MRJobAnalysisEntity> {
    private static final Logger LOG = LoggerFactory.getLogger(MRJobPerformanceAnalyzer.class);

    private static final String ANALYSIS_META_PATH = "analysis.meta";
    private static final String ANALYSIS_META_DEFAULT = "org.apache.eagle.jpm.analysis.mr.meta.impl.MetaManagementServiceMemoryImpl";

    private List<Evaluator> evaluators = new ArrayList<>();
    private List<Publisher> publishers = new ArrayList<>();
    private Config config;
    private MetaManagementService metaManagementService;

    public MRJobPerformanceAnalyzer(Config config) throws Exception {
        this.config = config;
        evaluators.add(new SLAJobEvaluator(config));
        evaluators.add(new JobSuggestionEvaluator(config));

        publishers.add(new EagleStorePublisher(config));
        publishers.add(new EmailPublisher(config));

        String metaClass = ANALYSIS_META_DEFAULT;
        if (config.hasPath(ANALYSIS_META_PATH)) {
            metaClass = config.getString(ANALYSIS_META_PATH);
        }

        Class<?> clz = Thread.currentThread().getContextClassLoader().loadClass(metaClass);
        if (MetaManagementService.class.isAssignableFrom(clz)) {
            Constructor<?> cotr = clz.getConstructor(Config.class);
            LOG.info("meta class loaded: " + metaClass);
            metaManagementService = (MetaManagementService)cotr.newInstance(config);
        } else {
            throw new Exception("failed to load class " + metaClass);
        }
    }

    @Override
    public void analysis(MRJobAnalysisEntity mrJobAnalysisEntity) throws Exception {
        Result result = new Result();

        JobMetaEntity jobMetaEntity = metaManagementService.getJobMeta(mrJobAnalysisEntity.getJobDefId());
        for (Evaluator evaluator : evaluators) {
            if (!jobMetaEntity.getEvaluators().contains(evaluator.getClass().getSimpleName())) {
                LOG.info("skip evaluator " + evaluator.getClass().getSimpleName());
                continue;
            }
            result.addEvaluatorResult(evaluator.getClass(), evaluator.evaluate(mrJobAnalysisEntity));
        }

        for (Publisher publisher : publishers) {
            publisher.publish(result);
        }
    }
}
