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
package org.apache.eagle.jpm.mr.history;

import backtype.storm.topology.BoltDeclarer;
import com.codahale.metrics.health.HealthCheck;
import org.apache.eagle.app.StormApplication;
import org.apache.eagle.app.environment.impl.StormEnvironment;
import org.apache.eagle.app.sink.StormStreamSink;
import org.apache.eagle.jpm.mr.history.crawler.JobHistoryContentFilter;
import org.apache.eagle.jpm.mr.history.crawler.JobHistoryContentFilterBuilder;
import org.apache.eagle.jpm.mr.history.storm.JobHistorySpout;
import org.apache.eagle.jpm.util.Constants;

import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import com.typesafe.config.Config;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

public class MRHistoryJobApplication extends StormApplication {
    @Override
    public StormTopology execute(Config config, StormEnvironment environment) {
        //1. trigger init conf
        MRHistoryJobConfig appConfig = MRHistoryJobConfig.newInstance(config);
        com.typesafe.config.Config jhfAppConf = appConfig.getConfig();

        //2. init JobHistoryContentFilter
        final JobHistoryContentFilterBuilder builder = JobHistoryContentFilterBuilder.newBuilder().acceptJobFile().acceptJobConfFile();
        String[] confKeyPatternsSplit = jhfAppConf.getString("MRConfigureKeys.jobConfigKey").split(",");
        List<String> confKeyPatterns = new ArrayList<>(confKeyPatternsSplit.length);
        for (String confKeyPattern : confKeyPatternsSplit) {
            confKeyPatterns.add(confKeyPattern.trim());
        }
        confKeyPatterns.add(Constants.JobConfiguration.CASCADING_JOB);
        confKeyPatterns.add(Constants.JobConfiguration.HIVE_JOB);
        confKeyPatterns.add(Constants.JobConfiguration.PIG_JOB);
        confKeyPatterns.add(Constants.JobConfiguration.SCOOBI_JOB);

        String jobNameKey = jhfAppConf.getString("MRConfigureKeys.jobNameKey");
        builder.setJobNameKey(jobNameKey);

        for (String key : confKeyPatterns) {
            builder.includeJobKeyPatterns(Pattern.compile(key));
        }
        JobHistoryContentFilter filter = builder.build();
        //3. init topology
        TopologyBuilder topologyBuilder = new TopologyBuilder();
        String spoutName = "mrHistoryJobSpout";
        int tasks = jhfAppConf.getInt("stormConfig.mrHistoryJobSpoutTasks");
        topologyBuilder.setSpout(
                spoutName,
                new JobHistorySpout(filter, appConfig),
                tasks
        ).setNumTasks(tasks);

        StormStreamSink sinkBolt = environment.getStreamSink("mr_failed_job_stream", config);
        BoltDeclarer kafkaBoltDeclarer = topologyBuilder.setBolt("HistoryKafkaSink", sinkBolt, jhfAppConf.getInt("stormConfig.historyKafkaSinkTasks"))
                .setNumTasks(jhfAppConf.getInt("stormConfig.historyKafkaSinkTasks"));
        kafkaBoltDeclarer.shuffleGrouping(spoutName);

        return topologyBuilder.createTopology();
    }
}
