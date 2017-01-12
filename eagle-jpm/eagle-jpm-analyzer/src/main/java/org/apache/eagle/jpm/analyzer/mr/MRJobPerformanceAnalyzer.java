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

package org.apache.eagle.jpm.analyzer.mr;

import com.typesafe.config.Config;
import org.apache.eagle.jpm.analyzer.*;
import org.apache.eagle.jpm.analyzer.Evaluator;
import org.apache.eagle.jpm.analyzer.mr.sla.SLAJobEvaluator;
import org.apache.eagle.jpm.analyzer.mr.suggestion.JobSuggestionEvaluator;
import org.apache.eagle.jpm.analyzer.publisher.EagleStorePublisher;
import org.apache.eagle.jpm.analyzer.publisher.EmailPublisher;
import org.apache.eagle.jpm.analyzer.publisher.Publisher;
import org.apache.eagle.jpm.analyzer.publisher.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class MRJobPerformanceAnalyzer implements JobAnalyzer, Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(MRJobPerformanceAnalyzer.class);

    private List<Evaluator> evaluators = new ArrayList<>();
    private List<Publisher> publishers = new ArrayList<>();

    private Config config;

    public MRJobPerformanceAnalyzer(Config config) {
        this.config = config;
        evaluators.add(new SLAJobEvaluator(config));
        evaluators.add(new JobSuggestionEvaluator(config));

        publishers.add(new EagleStorePublisher(config));
        publishers.add(new EmailPublisher(config));
    }

    @Override
    public void analyze(AnalyzerEntity analyzerJobEntity) throws Exception {
        Result result = new Result();

        for (Evaluator evaluator : evaluators) {
            result.addEvaluatorResult(evaluator.getClass(), evaluator.evaluate(analyzerJobEntity));
        }

        for (Publisher publisher : publishers) {
            publisher.publish(analyzerJobEntity, result);
        }
    }
}
