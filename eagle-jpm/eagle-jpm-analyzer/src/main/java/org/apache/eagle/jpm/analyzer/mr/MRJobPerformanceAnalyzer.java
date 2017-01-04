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
import org.apache.eagle.jpm.analyzer.mr.meta.model.JobMetaEntity;
import org.apache.eagle.jpm.analyzer.mr.sla.SLAJobEvaluator;
import org.apache.eagle.jpm.analyzer.mr.suggestion.JobSuggestionEvaluator;
import org.apache.eagle.jpm.analyzer.publisher.EagleStorePublisher;
import org.apache.eagle.jpm.analyzer.publisher.EmailPublisher;
import org.apache.eagle.jpm.analyzer.publisher.Publisher;
import org.apache.eagle.jpm.analyzer.publisher.Result;
import org.apache.eagle.jpm.util.Constants;
import org.apache.eagle.jpm.util.Utils;
import org.apache.eagle.jpm.util.resourcefetch.connection.InputStreamUtils;
import org.apache.eagle.metadata.resource.RESTResponse;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.io.Serializable;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.List;

public class MRJobPerformanceAnalyzer implements JobAnalyzer<MRJobAnalysisEntity>, Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(MRJobPerformanceAnalyzer.class);

    private static final ObjectMapper OBJ_MAPPER = new ObjectMapper();

    private static final String HOST_PATH = "service.host";
    private static final String PORT_PATH = "service.port";
    private static final String CONTEXT_PATH = "service.context";
    private static final String META_PATH = "/job/analyzer/meta";

    private List<Evaluator> evaluators = new ArrayList<>();
    private List<Publisher> publishers = new ArrayList<>();

    private Config config;

    static {
        OBJ_MAPPER.configure(JsonParser.Feature.ALLOW_NON_NUMERIC_NUMBERS, true);
    }

    public MRJobPerformanceAnalyzer(Config config) {
        this.config = config;
        evaluators.add(new SLAJobEvaluator(config));
        evaluators.add(new JobSuggestionEvaluator(config));

        publishers.add(new EagleStorePublisher(config));
        publishers.add(new EmailPublisher(config));
    }

    @Override
    public void analysis(MRJobAnalysisEntity mrJobAnalysisEntity) throws Exception {
        Result result = new Result();

        List<JobMetaEntity> jobMetaEntities = getJobMeta(mrJobAnalysisEntity.getJobDefId());
        for (Evaluator evaluator : evaluators) {
            if (jobMetaEntities.size() > 0 && !jobMetaEntities.get(0).getEvaluators().contains(evaluator.getClass().getSimpleName())) {
                LOG.info("skip evaluator " + evaluator.getClass().getSimpleName());
                //continue;
            }
            result.addEvaluatorResult(evaluator.getClass(), evaluator.evaluate(mrJobAnalysisEntity));
        }

        for (Publisher publisher : publishers) {
            publisher.publish(result);
        }
    }

    private List<JobMetaEntity> getJobMeta(String jobDefId) {
        List<JobMetaEntity> result = new ArrayList<>();
        String url = "http://"
                + config.getString(HOST_PATH)
                + ":"
                + config.getInt(PORT_PATH)
                + config.getString(CONTEXT_PATH)
                + META_PATH
                + "/"
                + URLEncoder.encode(jobDefId);

        InputStream is = null;
        try {
            is = InputStreamUtils.getInputStream(url, null, Constants.CompressionType.NONE);
            LOG.info("get job meta from {}", url);
            result = (List<JobMetaEntity>)OBJ_MAPPER.readValue(is, RESTResponse.class).getData();
        } catch (Exception e) {
            LOG.warn("failed to get job meta from {}", url, e);
        } finally {
            Utils.closeInputStream(is);
            return result;
        }
    }
}
