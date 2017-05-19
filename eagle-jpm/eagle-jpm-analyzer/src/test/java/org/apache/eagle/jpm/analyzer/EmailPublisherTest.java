/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.eagle.jpm.analyzer;

import com.dumbster.smtp.SimpleSmtpServer;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.eagle.jpm.analyzer.meta.model.AnalyzerEntity;
import org.apache.eagle.jpm.analyzer.mr.sla.SLAJobEvaluator;
import org.apache.eagle.jpm.analyzer.mr.suggestion.JobSuggestionEvaluator;
import org.apache.eagle.jpm.analyzer.mr.suggestion.MapReduceCompressionSettingProcessor;
import org.apache.eagle.jpm.analyzer.mr.suggestion.MapReduceDataSkewProcessor;
import org.apache.eagle.jpm.analyzer.publisher.EmailPublisher;
import org.apache.eagle.jpm.analyzer.publisher.Result;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EmailPublisherTest {

    private static final Logger LOG = LoggerFactory.getLogger(EmailPublisherTest.class);
    private static final int SMTP_PORT = 5025;
    private SimpleSmtpServer server;

    @Before
    public void setUp() {
        server = SimpleSmtpServer.start(SMTP_PORT);
    }

    @After
    public void clear() {
        if (server != null) {
            server.stop();
        }
    }

    @Test
    public void test() {
        AnalyzerEntity analyzerJobEntity = new AnalyzerEntity();
        analyzerJobEntity.setJobId("job1");

        Result.EvaluatorResult jobSuggestionResult = new Result.EvaluatorResult();
        jobSuggestionResult.addProcessorResult(MapReduceCompressionSettingProcessor.class, new Result.ProcessorResult(Result.RuleType.COMPRESS, Result.ResultLevel.INFO, "compress"));
        jobSuggestionResult.addProcessorResult(MapReduceDataSkewProcessor.class, new Result.ProcessorResult(Result.RuleType.DATA_SKEW, Result.ResultLevel.WARNING, "data skew"));

        Result.EvaluatorResult jobSlaResult = new Result.EvaluatorResult();
        jobSlaResult.addProcessorResult(Processor.class, new Result.ProcessorResult(Result.RuleType.LONG_DURATION_JOB, Result.ResultLevel.CRITICAL, "long running job"));

        Result result = new Result();
        result.addEvaluatorResult(JobSuggestionEvaluator.class, jobSuggestionResult);
        result.addEvaluatorResult(SLAJobEvaluator.class, jobSlaResult);

        Config config = ConfigFactory.load();
        EmailPublisher publisher = new EmailPublisher(config);
        publisher.publish(analyzerJobEntity, result);

        Assert.assertTrue(server.getReceivedEmailSize() == 1 );
        Assert.assertTrue(server.getReceivedEmail().hasNext());
    }


}
