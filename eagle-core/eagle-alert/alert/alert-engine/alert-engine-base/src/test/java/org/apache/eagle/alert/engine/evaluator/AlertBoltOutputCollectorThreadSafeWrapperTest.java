package org.apache.eagle.alert.engine.evaluator;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.eagle.alert.engine.evaluator.impl.AlertBoltOutputCollectorThreadSafeWrapper;
import org.apache.eagle.alert.engine.model.AlertStreamEvent;
import org.junit.Assert;
import org.junit.Test;

import backtype.storm.task.IOutputCollector;
import backtype.storm.task.OutputCollector;

public class AlertBoltOutputCollectorThreadSafeWrapperTest {
    @Test
    public void testThreadSafeAlertBoltOutputCollector(){
        MockedStormAlertOutputCollector stormOutputCollector = new MockedStormAlertOutputCollector(null);
        AlertBoltOutputCollectorThreadSafeWrapper alertBoltOutputCollectorWrapper = new AlertBoltOutputCollectorThreadSafeWrapper(stormOutputCollector);
        alertBoltOutputCollectorWrapper.emit(create("mockAlert_1"));
        alertBoltOutputCollectorWrapper.emit(create("mockAlert_2"));
        Assert.assertEquals(0,stormOutputCollector.getCollected().size());
        Assert.assertEquals(0,stormOutputCollector.getTupleSize());
        alertBoltOutputCollectorWrapper.flush();
        Assert.assertEquals(2,stormOutputCollector.getCollected().size());
        Assert.assertEquals(2,stormOutputCollector.getTupleSize());
        alertBoltOutputCollectorWrapper.emit(create("mockAlert_3"));
        Assert.assertEquals(2,stormOutputCollector.getCollected().size());
        Assert.assertEquals(2,stormOutputCollector.getTupleSize());
        alertBoltOutputCollectorWrapper.flush();
        alertBoltOutputCollectorWrapper.flush();
        alertBoltOutputCollectorWrapper.flush();
        Assert.assertEquals(3,stormOutputCollector.getCollected().size());
        Assert.assertEquals(3,stormOutputCollector.getTupleSize());
    }

    private AlertStreamEvent create(String streamId){
        AlertStreamEvent alert = new AlertStreamEvent();
        alert.setCreatedBy(this.toString());
        alert.setCreatedTime(System.currentTimeMillis());
        alert.setData(new Object[]{"field_1",2,"field_3"});
        alert.setStreamId(streamId);
        return alert;
    }

    private class MockedStormAlertOutputCollector extends OutputCollector {
        private final Map<Object,List<Object>> collected;
        MockedStormAlertOutputCollector(IOutputCollector delegate) {
            super(delegate);
            collected = new HashMap<>();
        }

        @Override
        public List<Integer> emit(String streamId, List<Object> tuple) {
            if(!collected.containsKey(tuple.get(0))){
                collected.put(tuple.get(0),new LinkedList<>());
            }
            collected.get(tuple.get(0)).add(tuple);
            return null;
        }
        Map<Object,List<Object>> getCollected(){
            return collected;
        }

        int getTupleSize(){
            int size = 0;
            for(List<Object> alerts:collected.values()){
                size += alerts.size();
            }
            return size;
        }
    }
}