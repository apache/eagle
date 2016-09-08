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
package org.apache.eagle.alert.engine.nodata;

import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.times;

import org.apache.eagle.alert.engine.evaluator.nodata.DistinctValuesInTimeBatchWindow;
import org.apache.eagle.alert.engine.evaluator.nodata.NoDataPolicyTimeBatchHandler;
import org.apache.eagle.alert.engine.model.StreamEvent;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestDistinctValuesInTimeBatchWindow {

    private static final String inputStream = "testInputStream";

    private NoDataPolicyTimeBatchHandler handler;

    @Before
    public void setup() {
        handler = mock(NoDataPolicyTimeBatchHandler.class);
    }

    @After
    public void teardown() {
    }

    @Test
    public void testNormal() throws Exception {
        // wisb is null since it is dynamic mode
        DistinctValuesInTimeBatchWindow window = new DistinctValuesInTimeBatchWindow(handler, 5 * 1000, null);

        long now = System.currentTimeMillis();

        // handler.compareAndEmit(anyObject(), anyObject(), anyObject());

        // event time
        sendEventToWindow(window, now, "host1", 95.5);

        Thread.sleep(6000);

        sendEventToWindow(window, now, "host1", 91.0);
        sendEventToWindow(window, now, "host2", 95.5);
        sendEventToWindow(window, now, "host2", 97.1);

        Thread.sleep(3000);

        sendEventToWindow(window, now, "host1", 90.7);

        Thread.sleep(4000);

        sendEventToWindow(window, now, "host1", 90.7);

        Thread.sleep(3000);

        verify(handler, times(3)).compareAndEmit(anyObject(), anyObject(), anyObject());
    }

    private void sendEventToWindow(DistinctValuesInTimeBatchWindow window, long ts, String host, double value) {
        window.send(buildStreamEvent(ts, host, value), host, ts);
    }

    private StreamEvent buildStreamEvent(long ts, String host, double value) {
        StreamEvent e = new StreamEvent();
        e.setData(new Object[] {ts, host, value});
        e.setStreamId(inputStream);
        e.setTimestamp(ts);
        return e;
    }

}
