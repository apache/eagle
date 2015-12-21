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
package org.apache.eagle.alert.cep;

import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;
import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.stream.output.StreamCallback;

import junit.framework.Assert;

/**
 * @since Dec 21, 2015
 *
 */
public class TestAggregation {

	/**
	 * <pre>
	 given any event schema, describe your aggregation and interpret that into siddhi language. 
	note1: support time downsampling, spatial rollup
	note2: metric semantics type, counter(number between the snapshots), gauge(snapshot value), total(number since start till this snapshot) 
	note3: does external time support time batch? (Libin?)
	note4: what is output for aggregation? If it is druid, then we need a following DruidPersistenceTask
	 * </pre>
	 */
    @Test
    public void test01DownSampling() throws Exception {
        String stream = "define stream jmxMetric(cpu int, memory int, bytesIn int, bytesOut long, timestamp long);";
        String query = "@info(name = 'downSample') " 
                + "from jmxMetric#window.timeBatch(3 sec) "
                + "select "
                + "avg(cpu) as avgCpu, max(cpu) as maxCpu, "
                + " avg(memory) as avgMem, max(memory) as maxMem, "
                + " avg(bytesIn) as avgBytesIn, max(bytesIn) as maxBytesIn, "
//                + " avg(bytesOut) as avgBytesOut, max(bytesOut) as maxBytesOut " 
//                + " , "
//                + " min(cpu) as minCpu, max(cpu) as maxCpu, avg(cpu) as avgCpu, "
                + " timestamp as timeWindowEnds "
                + " , "
                + " count(1) as errorCount "
//                + " output snapshot every 3 sec "
                // expired-events
                + " INSERT  INTO tmp;";

		SiddhiManager sm = new SiddhiManager();
		ExecutionPlanRuntime plan = sm.createExecutionPlanRuntime(stream + query);
		
		InputHandler input = plan.getInputHandler("jmxMetric");
		
		final AtomicInteger counter = new AtomicInteger();
		plan.addCallback("jmxMetric", new StreamCallback() {
			@Override
			public void receive(Event[] arg0) {
				int current = counter.addAndGet(arg0.length);
//				System.out.println(String.format("number %d event, with values : %s ", current, );
			}
		});
		
		plan.start();
		
		Thread.sleep(3000);
		sendEvent(input);
		Thread.sleep(3000);
		sendEvent(input);
		Thread.sleep(3000);
		sendEvent(input);
		
		Thread.sleep(5000);
		plan.shutdown();
		Assert.assertEquals(3, counter);
	}

	// send 3 events
	private void sendEvent(InputHandler input) throws Exception {
		int len = 3;
		Event[] events = new Event[len];
		for (int i = 0; i < len; i++) {
			long externalTs = System.currentTimeMillis();
			// cpu int, memory int, bytesIn long, bytesOut long, timestamp long
			events[i] = new Event(externalTs + i, new Object[] {
					15,
					15,
					1000,
					2000,
					externalTs + i
			});
		}
		
		for (Event e : events) {
			input.send(e);
		}
	}

}
