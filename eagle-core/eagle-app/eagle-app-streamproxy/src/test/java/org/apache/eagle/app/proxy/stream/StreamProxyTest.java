/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.eagle.app.proxy.stream;

import com.google.inject.Inject;
import com.typesafe.config.ConfigFactory;
import org.apache.eagle.app.messaging.KafkaStreamProvider;
import org.apache.eagle.app.messaging.KafkaStreamSinkConfig;
import org.apache.eagle.app.messaging.StreamRecord;
import org.apache.eagle.app.proxy.stream.impl.StreamProxyImpl;
import org.apache.eagle.app.test.ApplicationTestBase;
import org.apache.eagle.app.test.KafkaTestServer;
import org.apache.eagle.metadata.model.StreamDesc;
import org.junit.*;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.Collections;

public class StreamProxyTest extends ApplicationTestBase {
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Inject
    private StreamProxyManager proxyManager;
    private KafkaTestServer kafkaTestServer;
    private StreamDesc streamDesc;

    @Before
    public void before() throws Exception {
        kafkaTestServer = KafkaTestServer.create(temporaryFolder.newFolder());
        kafkaTestServer.start();
        this.streamDesc = new StreamDesc();
        this.streamDesc.setStreamId("TEST_METRIC_STREAM");
        KafkaStreamSinkConfig sinkConfig = new KafkaStreamProvider().getSinkConfig("TEST_METRIC_STREAM", ConfigFactory.load());
        this.streamDesc.setSinkConfig(sinkConfig);
    }

    @After
    public void after() throws IOException {
        kafkaTestServer.stop();
    }

    @Test
    public void testProxyManagerInjection() {
        Assert.assertNotNull(proxyManager);
    }

    @Test
    public void testStreamProxyProduce() throws IOException {
        StreamProxy streamProxy = new StreamProxyImpl();
        streamProxy.open(streamDesc);
        streamProxy.send(Collections.singletonList(new StreamRecord() {
            {
                put("metric", "DiskUsage");
                put("host", "localhost");
                put("value", 98);
            }
        }));
        streamProxy.close();
    }
}
