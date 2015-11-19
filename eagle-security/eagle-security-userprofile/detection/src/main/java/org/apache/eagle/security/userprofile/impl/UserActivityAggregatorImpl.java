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
package org.apache.eagle.security.userprofile.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.eagle.common.DateTimeUtil;
import org.apache.eagle.datastream.Collector;
import org.apache.eagle.datastream.Tuple2;
import org.apache.eagle.security.userprofile.TimeWindow;
import org.apache.eagle.security.userprofile.UserActivityAggregator;
import org.apache.eagle.security.userprofile.UserProfileConstants;
import org.apache.eagle.security.userprofile.UserProfileUtils;
import org.apache.eagle.security.userprofile.model.UserActivityAggModelEntity;
import org.joda.time.Period;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * UserActivityAggregator implementation
 *
 * @see org.apache.eagle.security.userprofile.UserActivityAggregator
 * @since 9/28/15
 */
public class UserActivityAggregatorImpl implements UserActivityAggregator {
    private final Map<Long,TimeWindow> timeWindowCache = Collections.synchronizedMap(new HashMap<Long, TimeWindow>());
    private final Map<TimeWindow,Map<Tuple2<String,String>,Double>> counterCache = Collections.synchronizedMap(new HashMap<TimeWindow, Map<Tuple2<String, String>, Double>>());
    private final static Logger LOG = LoggerFactory.getLogger(UserActivityAggregatorImpl.class);
    private final String site;
    private final long safeWindowMs;
    private final Period granularity;
    private final List<String> cmdFeatures;

    public UserActivityAggregatorImpl(List<String> cmdFeatures,Period granularity,String site,long safeWindowMs){
        this.cmdFeatures = cmdFeatures;
        this.granularity = granularity;
        this.site = site;
        this.safeWindowMs = safeWindowMs;
    }

    /**
     * Build new time window
     *
     * @param from where time window start from
     * @param to where time window end at
     * @return new time window instance
     */
    private TimeWindow newTimeWindowInstance(Long from, Long to){
        return new TimeWindowImpl(from,to,this.safeWindowMs);
    }

    @Override
    public void accumulate(Map<String, Object> event, Collector<Tuple2<String, UserActivityAggModelEntity>> collector) {
        Long timestamp = (Long) event.get("timestamp");
        Long baseTimestamp = UserProfileUtils.formatMillisecondsByPeriod(timestamp, this.granularity);

        TimeWindow window = timeWindowCache.get(baseTimestamp);
        if(window == null) {
            window = newTimeWindowInstance(baseTimestamp, baseTimestamp + this.granularity.toStandardSeconds().getSeconds() * 1000);
            LOG.info(String.format("Creating %s",window.toString()));
            // Initialize time window
            init(window);
        }
        List<TimeWindow> expiredWindows = new ArrayList<>();
        for(TimeWindow win:timeWindowCache.values()){
            if(win.accept(timestamp)) accumulate(win, event);
            if(win.expire()){
                // Flush time window
                flush(win, collector);
                expiredWindows.add(win);
            }
        }

        // clean expired time window
        for(TimeWindow win:expiredWindows){
            if(LOG.isDebugEnabled()) LOG.debug(String.format("%s is expired at [%s]", win.toString(), DateTimeUtil.millisecondsToHumanDateWithMilliseconds(timestamp)));
            close(win);
        }
    }

    private void close(TimeWindow win) {
        counterCache.remove(win);
        timeWindowCache.remove(win.from());
    }

    private void init(TimeWindow window) {
        timeWindowCache.put(window.from(),window);
        Map<Tuple2<String,String>,Double> counter = counterCache.get(window);
        if(counter == null){
            counter = new HashMap<>();
            counterCache.put(window,counter);
        }
    }

    private void accumulate(TimeWindow window, Map<String, Object> event) {
        Map<Tuple2<String,String>,Double> counter = counterCache.get(window);
        String user = (String) event.get("user");
        String cmd = (String) event.get("cmd");
        Tuple2<String,String> tuple = new Tuple2<>(user,cmd);
        if(counter.containsKey(tuple)){
            Double count = counter.get(tuple) + 1;
            counter.put(tuple,count);
        }else{
            counter.put(tuple,1.0);
        }
    }

    private void flush(TimeWindow window, Collector<Tuple2<String, UserActivityAggModelEntity>> collector){
        Map<Tuple2<String,String>,Double> counter = counterCache.get(window);

        Map<String,Map<String,Double>> tmp = new HashMap<>();
        for(Map.Entry<Tuple2<String,String>,Double> entry:counter.entrySet()){
            String user = entry.getKey().f0();
            Map<String,Double> cmdCount = tmp.get(user);
            if(cmdCount == null) cmdCount = new HashMap<>();
            cmdCount.put(entry.getKey().f1(),entry.getValue());
            tmp.put(user,cmdCount);
        }

        for(Map.Entry<String,Map<String,Double>> entry:tmp.entrySet()){
            final String user = entry.getKey();
            final Map<String,Double> cmdCountMap = entry.getValue();

            UserActivityAggModelEntity model = new UserActivityAggModelEntity();
            model.setTags(new HashMap<String, String>(){{
                put(UserProfileConstants.SITE_TAG,site);
                put(UserProfileConstants.USER_TAG,user);
            }});

            // Build matrix
            double[][] matrix = new double[1][this.cmdFeatures.size()];

            for(int i=0;i<cmdFeatures.size();i++){
                Double cnt = cmdCountMap.get(cmdFeatures.get(i));
                if(cnt == null) cnt = 0.0;
                matrix[0][i] = cnt;
            }
            model.setCmdTypes(cmdFeatures);
            model.setCmdMatrix(matrix);
            model.setTimestamp(window.to());

            if(LOG.isDebugEnabled()) {
                try {
                    LOG.debug(new ObjectMapper().writeValueAsString(model));
                } catch (JsonProcessingException e) {
                    LOG.error(e.getMessage(), e);
                }
            }
            collector.collect(new Tuple2(user, model));
        }
        LOG.info(String.format("Flushed %s records during %s", tmp.size(), window));
    }
}