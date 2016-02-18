/*
 *
 *    Licensed to the Apache Software Foundation (ASF) under one or more
 *    contributor license agreements.  See the NOTICE file distributed with
 *    this work for additional information regarding copyright ownership.
 *    The ASF licenses this file to You under the Apache License, Version 2.0
 *    (the "License"); you may not use this file except in compliance with
 *    the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 *
 */

package org.apache.eagle.gc.executor;

import com.typesafe.config.Config;
import org.apache.eagle.datastream.Collector;
import org.apache.eagle.datastream.JavaStormStreamExecutor2;
import org.apache.eagle.gc.model.GCPausedEvent;
import org.apache.eagle.gc.stream.GCStreamBuilder;
import org.apache.eagle.gc.parser.exception.IgnoredLogFormatException;
import org.apache.eagle.gc.parser.exception.UnrecognizedLogFormatException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.List;
import java.util.Map;

public class GCLogAnalysorExecutor extends JavaStormStreamExecutor2<String, Map> {

    public final static Logger LOG = LoggerFactory.getLogger(GCLogAnalysorExecutor.class);

    private Config config;

    private long previousLogTime;

    @Override
    public void prepareConfig(Config config) {
        this.config = config;
    }

    @Override
    public void init() {

    }

    @Override
    public void flatMap(List<Object> input, Collector<Tuple2<String, Map>> collector) {
        String log = (String)input.get(0);
        GCStreamBuilder builder = new GCStreamBuilder();
        try {
            GCPausedEvent pauseEvent = builder.build(log);
            // Because some gc log like concurrent mode failure may miss timestamp info, so we set the previous log's timestamp for it
            if (pauseEvent.getTimestamp() == 0) {
                pauseEvent.setTimestamp(previousLogTime);
            }
            previousLogTime = pauseEvent.getTimestamp();
            collector.collect(new Tuple2("GCLog", pauseEvent.toMap()));
        }
        catch (IgnoredLogFormatException ex1) {
            //DO nothing
        }
        catch (UnrecognizedLogFormatException ex2) {
            LOG.warn(ex2.getMessage());
        }
        catch (Exception ex3) {
            LOG.error("Got an exception when parsing log: ", ex3);
        }
    }
}