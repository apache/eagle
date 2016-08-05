/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.eagle.app.sink;

import backtype.storm.task.TopologyContext;
import com.typesafe.config.Config;
import org.apache.eagle.alert.engine.model.StreamEvent;
import org.apache.eagle.app.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class KafkaStreamSink extends StormStreamSink<KafkaStreamSinkConfig> {
    private final static Logger LOGGER = LoggerFactory.getLogger(KafkaStreamSink.class);
    private String topicId;

    @Override
    public void init(String streamId, KafkaStreamSinkConfig config) {
        super.init(streamId, config);
        this.topicId = config.getTopicId();
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        super.prepare(stormConf, context);
        // TODO: Create KafkaProducer
    }

    @Override
    protected void onEvent(StreamEvent streamEvent) {
        LOGGER.info("TODO: producing {} to '{}'",streamEvent,topicId);
    }

    @Override
    public void onInstall() {
        ensureTopicCreated();
    }

    private void ensureTopicCreated(){
        LOGGER.info("TODO: ensure kafka topic {} created",this.topicId);
    }

    private void ensureTopicDeleted(){
        LOGGER.info("TODO: ensure kafka topic {} deleted",this.topicId);
    }

    @Override
    public void onUninstall() {
        ensureTopicDeleted();
    }

    public static class Provider implements StreamSinkProvider<KafkaStreamSink,KafkaStreamSinkConfig> {
        @Override
        public KafkaStreamSinkConfig getSinkConfig(String streamId, Config config) {
            String topicId = String.format("EAGLE.%s.%s",
                    config.getString("siteId"),
                    streamId).toLowerCase();
            KafkaStreamSinkConfig desc = new KafkaStreamSinkConfig();
            desc.setTopicId(topicId);
            return desc;
        }

        @Override
        public KafkaStreamSink getSink() {
            return new KafkaStreamSink();
        }
    }
}
