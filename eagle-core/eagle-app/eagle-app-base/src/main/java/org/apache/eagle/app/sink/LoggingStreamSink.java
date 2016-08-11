/**
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

import backtype.storm.topology.BasicOutputCollector;
import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class LoggingStreamSink extends StormStreamSink<DefaultStreamSinkConfig> {
    private final static Logger LOGGER = LoggerFactory.getLogger(KafkaStreamSink.class);

    @Override
    public void onInstall() {
        LOGGER.info("Executing onInstall callback, do nothing");
    }

    @Override
    public void onUninstall() {
        LOGGER.info("Executing onUninstall callback, do nothing");
    }

    @Override
    protected void execute(Object key, Map event, BasicOutputCollector collector) {
        LOGGER.info("Receiving {}",event);
    }

    public static class Provider implements StreamSinkProvider<LoggingStreamSink,DefaultStreamSinkConfig> {
        @Override
        public DefaultStreamSinkConfig getSinkConfig(String streamId, Config config) {
            return new DefaultStreamSinkConfig(LoggingStreamSink.class);
        }

        @Override
        public LoggingStreamSink getSink() {
            return new LoggingStreamSink();
        }
    }
}
