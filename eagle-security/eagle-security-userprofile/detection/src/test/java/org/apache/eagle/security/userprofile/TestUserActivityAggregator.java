package org.apache.eagle.security.userprofile;

import org.apache.eagle.common.DateTimeUtil;
import org.apache.eagle.datastream.Collector;
import org.apache.eagle.datastream.Tuple2;
import org.apache.eagle.security.userprofile.impl.UserActivityAggregatorImpl;
import org.apache.eagle.security.userprofile.model.UserActivityAggModelEntity;
import org.joda.time.Period;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

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
        Assert.assertEquals("user2",collector.getResult().get(0).f0());
        Assert.assertEquals("user1",collector.getResult().get(1).f0());
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
        Assert.assertEquals("user2",collector.getResult().get(0).f0());
        Assert.assertEquals("user1",collector.getResult().get(1).f0());
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
