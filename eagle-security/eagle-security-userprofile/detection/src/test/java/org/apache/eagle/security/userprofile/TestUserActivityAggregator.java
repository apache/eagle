/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * 
 */
package org.apache.eagle.security.userprofile;

import org.apache.eagle.common.DateTimeUtil;
import org.apache.eagle.datastream.Collector;
import org.apache.eagle.security.userprofile.impl.UserActivityAggregatorImpl;
import org.apache.eagle.security.userprofile.model.UserActivityAggModelEntity;
import org.joda.time.Period;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import scala.Tuple2;

import java.text.ParseException;
import java.util.*;


public class TestUserActivityAggregator {
    private UserActivityAggregator aggregator_0;
    private UserActivityAggregator aggregator_1;

    @Before
    public void setUp(){
        aggregator_0 = new UserActivityAggregatorImpl(Arrays.asList(UserProfileConstants.DEFAULT_CMD_TYPES),Period.parse("PT1m"),"test",0);
        aggregator_1 = new UserActivityAggregatorImpl(Arrays.asList(UserProfileConstants.DEFAULT_CMD_TYPES),Period.parse("PT1m"), "test", 5000);
    }

    @Test
    public void testAggWithoutSafeWindow() throws ParseException {
        SimpleCollector collector = new SimpleCollector();
        // yyyy-MM-dd HH:mm:ss,SSS
        aggregator_0.accumulate(event(DateTimeUtil.humanDateToMilliseconds("2015-09-29 01:01:00,000"),"user1","open"),collector);
        aggregator_0.accumulate(event(DateTimeUtil.humanDateToMilliseconds("2015-09-29 01:01:01,000"),"user1","open"),collector);
        aggregator_0.accumulate(event(DateTimeUtil.humanDateToMilliseconds("2015-09-29 01:01:02,000"),"user1","open"),collector);

        aggregator_0.accumulate(event(DateTimeUtil.humanDateToMilliseconds("2015-09-29 01:01:59,000"),"user2","open"),collector);

        aggregator_0.accumulate(event(DateTimeUtil.humanDateToMilliseconds("2015-09-29 01:02:01,000"),"user1","open"),collector);
        aggregator_0.accumulate(event(DateTimeUtil.humanDateToMilliseconds("2015-09-29 01:02:02,000"),"user2","open"),collector);

        Assert.assertEquals(2, collector.getResult().size());
        Assert.assertEquals("user2",collector.getResult().get(0)._1());
        Assert.assertEquals("user1",collector.getResult().get(1)._1());
    }

    @Test
    public void testAggWithSafeWindow() throws ParseException {
        SimpleCollector collector = new SimpleCollector();
        aggregator_1.accumulate(event(DateTimeUtil.humanDateToMilliseconds("2015-09-29 01:01:00,000"),"user1","open"),collector);  // create window
        aggregator_1.accumulate(event(DateTimeUtil.humanDateToMilliseconds("2015-09-29 01:01:01,000"),"user1","rename"),collector);
        aggregator_1.accumulate(event(DateTimeUtil.humanDateToMilliseconds("2015-09-29 01:01:59,000"),"user1","delete"),collector);

        aggregator_1.accumulate(event(DateTimeUtil.humanDateToMilliseconds("2015-09-29 01:02:01,000"),"user1","open"),collector);
        aggregator_1.accumulate(event(DateTimeUtil.humanDateToMilliseconds("2015-09-29 01:01:58,000"),"user2","open"),collector); // inside safe-window

        aggregator_1.accumulate(event(DateTimeUtil.humanDateToMilliseconds("2015-09-29 01:02:06,000"),"user2","open"),collector); //

        aggregator_1.accumulate(event(DateTimeUtil.humanDateToMilliseconds("2015-09-29 01:06:02,000"),"user2","open"),collector); //  outside safe-window

        Assert.assertEquals(4, collector.getResult().size());
        Assert.assertEquals("user2",collector.getResult().get(0)._1());
        Assert.assertEquals("user1",collector.getResult().get(1)._1());
    }


    private Map<String,Object> event(final long timestamp, final String user, final String cmd){
        return new HashMap<String,Object>(){{
            put("timestamp",timestamp);
            put("user",user);
            put("cmd",cmd);
        }};
    }

    private class SimpleCollector implements Collector<Tuple2<String, UserActivityAggModelEntity>>{
        private List<Tuple2<String, UserActivityAggModelEntity>> result = new ArrayList<>();
        @Override
        public void collect(Tuple2<String, UserActivityAggModelEntity> tuple) {
            result.add(tuple);
        }

        public List<Tuple2<String, UserActivityAggModelEntity>> getResult(){
            return result;
        }
    }
}