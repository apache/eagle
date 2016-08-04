package org.apache.eagle.common;

import org.junit.Test;
import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
import org.wso2.siddhi.core.stream.input.InputHandler;

/**
 * Since 8/3/16.
 */
public class TestSiddhiAggregator {
    @Test
    public void testSiddhi() throws Exception{
        String ql = "define stream s (host string, timestamp long, metric string, site string, value double);" +
                " @info(name='query') " +
                " from s[metric == \"missingblocks\"]#window.externalTimeBatch(timestamp, 3 sec) select host, count(value) as avg group by host insert into tmp; ";
        SiddhiManager sm = new SiddhiManager();
        ExecutionPlanRuntime runtime = sm.createExecutionPlanRuntime(ql);

        InputHandler input = runtime.getInputHandler("s");

        runtime.addCallback("query", new QueryCallback() {
                @Override
                public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                    for(Event e : inEvents) {
                        System.out.print(e);
                        System.out.print(",");
                    }
                    System.out.println();
                }
            });
        runtime.start();

        sendEvents(3, 4, 2, input, 1000L);
        Thread.sleep(1000);
        sendEvents(1, 2, 2, input, 11000L);
        runtime.shutdown();
        sm.shutdown();
        Thread.sleep(2000);
    }

    void sendEvents(int countHost1, int countHost2, int countHost3, InputHandler input, long startTime) throws Exception{
        for(int i=0; i<countHost1; i++){
            Event e = createEvent("host1", startTime + i*100);
            input.send(e);
        }
        startTime += 2000;
//        Thread.sleep(1000);
        for(int i=0; i<countHost2; i++){
            Event e = createEvent("host2", startTime + i*100);
            input.send(e);
        }
        startTime += 4000;
//        Thread.sleep(1000);
        for(int i=0; i<countHost3; i++){
            Event e = createEvent("host3", startTime + i*100);
            input.send(e);
        }
    }

    Event createEvent(String host, long timestamp){
        Event e = new Event();
        e.setTimestamp(timestamp);
        e.setData(new Object[]{host, timestamp, "missingblocks", "site1", 14.0});
        return e;
    }
}
