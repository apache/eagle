package org.apache.eagle.alert.engine;

import backtype.storm.metric.api.MultiCountMetric;

public class StormMultiCountMetric implements StreamCounter {
    private MultiCountMetric countMetric;

    public StormMultiCountMetric(MultiCountMetric counter) {
        this.countMetric = counter;
    }

    @Override
    public void incr(String scopeName) {
        countMetric.scope(scopeName).incr();
    }

    @Override
    public void incrBy(String scopeName, int length) {
        countMetric.scope(scopeName).incrBy(length);
    }

    @Override
    public void scope(String scopeName) {
        countMetric.scope(scopeName);
    }
}
