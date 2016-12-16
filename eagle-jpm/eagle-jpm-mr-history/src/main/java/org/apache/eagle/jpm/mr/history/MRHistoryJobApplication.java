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
import org.apache.eagle.app.StormApplication;
import org.apache.eagle.app.environment.impl.StormEnvironment;
import org.apache.eagle.app.messaging.StormStreamSink;
import org.apache.eagle.jpm.mr.history.crawler.JobHistoryContentFilter;
import org.apache.eagle.jpm.mr.history.crawler.JobHistoryContentFilterBuilder;
import org.apache.eagle.jpm.mr.history.crawler.JobHistorySpoutCollectorInterceptor;
import org.apache.eagle.jpm.mr.history.publisher.JobStreamPublisher;
import org.apache.eagle.jpm.mr.history.publisher.StreamPublisher;
import org.apache.eagle.jpm.mr.history.publisher.StreamPublisherManager;
import org.apache.eagle.jpm.mr.history.publisher.TaskAttemptStreamPublisher;
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
        //1. trigger prepare conf
        MRHistoryJobConfig appConfig = MRHistoryJobConfig.newInstance(config);
        com.typesafe.config.Config jhfAppConf = appConfig.getConfig();

        //2. prepare JobHistoryContentFilter
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
        //3. prepare topology
        TopologyBuilder topologyBuilder = new TopologyBuilder();
        String spoutName = "mrHistoryJobSpout";
        int tasks = jhfAppConf.getInt("stormConfig.mrHistoryJobSpoutTasks");
        JobHistorySpout jobHistorySpout = new JobHistorySpout(filter, appConfig);
        topologyBuilder.setSpout(
                spoutName,
                jobHistorySpout,
                tasks
        ).setNumTasks(tasks);

        StormStreamSink jobSinkBolt = environment.getStreamSink("MAP_REDUCE_JOB_STREAM", config);
        String jobSinkBoltName = "JobKafkaSink";
        BoltDeclarer jobKafkaBoltDeclarer = topologyBuilder.setBolt(jobSinkBoltName, jobSinkBolt, jhfAppConf.getInt("stormConfig.jobKafkaSinkTasks"))
                .setNumTasks(jhfAppConf.getInt("stormConfig.jobKafkaSinkTasks"));
        String spoutToJobSinkName = spoutName + "_to_" + jobSinkBoltName;
        jobKafkaBoltDeclarer.shuffleGrouping(spoutName, spoutToJobSinkName);

        StormStreamSink taskAttemptSinkBolt = environment.getStreamSink("MAP_REDUCE_TASK_ATTEMPT_STREAM", config);
        String taskAttemptSinkBoltName = "TaskAttemptKafkaSink";
        BoltDeclarer taskAttemptKafkaBoltDeclarer = topologyBuilder.setBolt(taskAttemptSinkBoltName, taskAttemptSinkBolt, jhfAppConf.getInt("stormConfig.taskAttemptKafkaSinkTasks"))
                .setNumTasks(jhfAppConf.getInt("stormConfig.taskAttemptKafkaSinkTasks"));
        String spoutToTaskAttemptSinkName = spoutName + "_to_" + taskAttemptSinkBoltName;
        taskAttemptKafkaBoltDeclarer.shuffleGrouping(spoutName, spoutToTaskAttemptSinkName);

        List<StreamPublisher> streamPublishers = new ArrayList<>();
        streamPublishers.add(new JobStreamPublisher(spoutToJobSinkName));
        streamPublishers.add(new TaskAttemptStreamPublisher(spoutToTaskAttemptSinkName));
        jobHistorySpout.setStreamPublishers(streamPublishers);

        return topologyBuilder.createTopology();
    }
}
