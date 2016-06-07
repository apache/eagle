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
package org.apache.eagle.alert.metric.sink;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.slf4j.LoggerFactory;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Slf4jReporter;
import com.typesafe.config.Config;

public class Slf4jSink implements MetricSink {
    private Slf4jReporter reporter;

    @SuppressWarnings("serial")
    private final static Map<String,Slf4jReporter.LoggingLevel> LEVEL_MAPPING = new HashMap<String,Slf4jReporter.LoggingLevel>(){{
        put("INFO",Slf4jReporter.LoggingLevel.INFO);
        put("DEBUG",Slf4jReporter.LoggingLevel.DEBUG);
        put("ERROR",Slf4jReporter.LoggingLevel.ERROR);
        put("TRACE",Slf4jReporter.LoggingLevel.TRACE);
        put("WARN",Slf4jReporter.LoggingLevel.WARN);
    }};

    private static Slf4jReporter.LoggingLevel getLoggingLevel(String level){
        if(LEVEL_MAPPING.containsKey(level.toUpperCase())){
            return LEVEL_MAPPING.get(level.toUpperCase());
        } else{
            throw new IllegalArgumentException("Illegal logging level: "+level);
        }
    }

    @Override
    public void prepare(Config config, MetricRegistry registry) {
        reporter = Slf4jReporter.forRegistry(registry)
                .outputTo(LoggerFactory.getLogger("org.apache.eagle.alert.metric"))
                .withLoggingLevel(config.hasPath("level")? getLoggingLevel(config.getString("level")): Slf4jReporter.LoggingLevel.INFO)
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .build();
    }

    @Override
    public void start(long period,TimeUnit unit) {
        reporter.start(period,unit);
    }

    @Override
    public void stop() {
        reporter.stop();
        reporter.close();
    }

    @Override
    public void report() {
        reporter.report();
    }
}
