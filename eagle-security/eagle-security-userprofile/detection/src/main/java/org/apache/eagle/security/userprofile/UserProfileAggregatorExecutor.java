/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.eagle.security.userprofile;

import com.typesafe.config.Config;
import org.apache.commons.lang3.StringUtils;
import org.apache.eagle.datastream.Collector;
import org.apache.eagle.datastream.JavaStormStreamExecutor2;
import org.apache.eagle.datastream.Tuple2;
import org.apache.eagle.security.userprofile.impl.UserActivityAggregatorImpl;
import org.apache.eagle.security.userprofile.model.UserActivityAggModelEntity;
import org.joda.time.Period;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * @since 9/29/15
 */
public class UserProfileAggregatorExecutor extends JavaStormStreamExecutor2<String, UserActivityAggModelEntity> {
    private static final Logger LOG = LoggerFactory.getLogger(UserProfileAggregatorExecutor.class);
    private UserActivityAggregator aggregator;
    private String site;
    private String granularity;
    private long safeWindowMs = 5000l; // 5s

    private String[] cmdFeatures = UserProfileConstants.DEFAULT_CMD_TYPES;

    @Override
    public void prepareConfig(Config config) {
        this.site = config.getString("eagleProps.site");
        if(config.hasPath("eagleProps.userProfileGranularity")){
            this.granularity = config.getString("eagleProps.userProfileGranularity");
            LOG.info("Override [userProfileGranularity] = [PT1m]");
        }else{
            LOG.info("Using default [userProfileGranularity] = [PT1m]");
            this.granularity = "PT1m";
        }
        if(config.hasPath("eagleProps.userProfileSafeWindowMs")){
            this.safeWindowMs = config.getLong("eagleProps.userProfileSafeWindowMs");
        }
        if(config.hasPath("eagleProps.userProfileCmdFeatures")){
            String cmdFeaturesStr = config.getString("eagleProps.userProfileCmdFeatures");
            LOG.info(String.format("Override [userProfileCmdFeatures] = [%s]",cmdFeaturesStr));
            this.cmdFeatures = cmdFeaturesStr.split(",");
        }else{
            LOG.info(String.format("Using default [userProfileCmdFeatures] = [%s]", StringUtils.join(this.cmdFeatures,",")));
        }
    }

    @Override
    public void init() {
        aggregator = new UserActivityAggregatorImpl(Arrays.asList(cmdFeatures),Period.parse(this.granularity),site,this.safeWindowMs);
    }

    @Override
    public void flatMap(List<Object> input, Collector<Tuple2<String, UserActivityAggModelEntity>> collector) {
        String user = (String) input.get(0);
        Map<String,Object> event = (Map<String, Object>) input.get(1);
        aggregator.accumulate(event,collector);
    }
}