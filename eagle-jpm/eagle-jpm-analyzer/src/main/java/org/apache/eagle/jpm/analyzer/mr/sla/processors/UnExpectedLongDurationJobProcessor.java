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

package org.apache.eagle.jpm.analyzer.mr.sla.processors;

import com.typesafe.config.Config;
import org.apache.eagle.jpm.analyzer.meta.model.AnalyzerEntity;
import org.apache.eagle.jpm.analyzer.publisher.Result;
import org.apache.eagle.jpm.analyzer.Processor;
import org.apache.eagle.jpm.analyzer.util.Constants;
import org.apache.eagle.jpm.analyzer.util.Utils;
import org.apache.eagle.log.entity.GenericServiceAPIResponseEntity;
import org.apache.eagle.service.client.IEagleServiceClient;
import org.apache.eagle.service.client.impl.EagleServiceClientImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.net.URLEncoder;
import java.util.List;
import java.util.Map;

public class UnExpectedLongDurationJobProcessor implements Processor, Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(UnExpectedLongDurationJobProcessor.class);

    private Config config;

    public UnExpectedLongDurationJobProcessor(Config config) {
        this.config = config;
    }

    @Override
    public Result.ProcessorResult process(AnalyzerEntity analyzerJobEntity) {
        LOG.info("Job {} In UnExpectedLongDurationJobProcessor", analyzerJobEntity.getJobDefId());

        Map<String, Object> jobMetaData = analyzerJobEntity.getJobMeta().getConfiguration();
        long avgDurationTime = getAvgDuration(analyzerJobEntity, jobMetaData);
        if (avgDurationTime == 0L) {
            return new Result.ProcessorResult(Result.RuleType.LONG_DURATION_JOB, Result.ResultLevel.NONE, Constants.PROCESS_NONE);
        }

        Map<Result.ResultLevel, Double> alertThreshold = Constants.DEFAULT_ALERT_THRESHOLD;
        if (jobMetaData.containsKey(Constants.ALERT_THRESHOLD_KEY)) {
            alertThreshold = (Map<Result.ResultLevel, Double>)jobMetaData.get(Constants.ALERT_THRESHOLD_KEY);
        }
        List<Map.Entry<Result.ResultLevel, Double>> sorted = Utils.sortByValue(alertThreshold);

        double expirePercent = (analyzerJobEntity.getDurationTime() - avgDurationTime) * 1.0 / avgDurationTime;
        for (Map.Entry<Result.ResultLevel, Double> entry : sorted) {
            if (expirePercent >= entry.getValue()) {
                return new Result.ProcessorResult(Result.RuleType.LONG_DURATION_JOB, entry.getKey(), String.format("Job duration exceeds average duration by %d%%, average duration is %ds",
                        (int)(expirePercent * 100), avgDurationTime / 1000));
            }
        }

        return new Result.ProcessorResult(Result.RuleType.LONG_DURATION_JOB, Result.ResultLevel.NONE, Constants.PROCESS_NONE);
    }

    private long getAvgDuration(AnalyzerEntity mrJobAnalysisEntity, Map<String, Object> jobMetaData) {
        IEagleServiceClient client = new EagleServiceClientImpl(
                config.getString(Constants.HOST_PATH),
                config.getInt(Constants.PORT_PATH),
                config.getString(Constants.USERNAME_PATH),
                config.getString(Constants.PASSWORD_PATH));

        client.setReadTimeout(config.getInt(Constants.READ_TIMEOUT_PATH) * 1000);

        try {
            int timeLength = Constants.DEFAULT_EVALUATOR_TIME_LENGTH;
            try {
                if (jobMetaData.containsKey(Constants.EVALUATOR_TIME_LENGTH_KEY)) {
                    timeLength = Integer.parseInt(jobMetaData.get(Constants.EVALUATOR_TIME_LENGTH_KEY).toString());
                }
            } catch (Exception e) {
                LOG.warn("exception found when parse timeLength {}, use default", e);
            }

            String query = String.format("%s[@site=\"%s\" and @jobDefId=\"%s\"]<@site>{avg(durationTime)}",
                    org.apache.eagle.jpm.util.Constants.MR_JOB_EXECUTION_SERVICE_NAME,
                    mrJobAnalysisEntity.getSiteId(),
                    URLEncoder.encode(mrJobAnalysisEntity.getJobDefId()));

            GenericServiceAPIResponseEntity response = client
                    .search(query)
                    .startTime(System.currentTimeMillis() - (timeLength + 1) * 24 * 60 * 60000L)
                    .endTime(System.currentTimeMillis() - 24 * 60 * 60000L)
                    .pageSize(10)
                    .send();

            List<Map<List<String>, List<Double>>> results = response.getObj();
            if (results.size() == 0) {
                return 0L;
            }
            return results.get(0).get("value").get(0).longValue();
        } catch (Exception e) {
            LOG.warn("{}", e);
            return 0L;
        } finally {
            try {
                client.close();
            } catch (Exception e) {
                LOG.warn("{}", e);
            }
        }
    }
}
