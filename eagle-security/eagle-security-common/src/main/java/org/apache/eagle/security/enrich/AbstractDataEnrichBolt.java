/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.eagle.security.enrich;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Since 8/16/16.
 */
public abstract class AbstractDataEnrichBolt<T, K> extends BaseRichBolt {
    private final static Logger LOG = LoggerFactory.getLogger(AbstractDataEnrichBolt.class);
    protected Config config;
    protected OutputCollector collector;
    private DataEnrichLCM<T, K> lcm;

    public AbstractDataEnrichBolt(Config config, DataEnrichLCM<T, K> lcm){
        this.config = config;
        this.lcm = lcm;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        // start external data retrieval
        try {
            ExternalDataJoiner joiner = new ExternalDataJoiner(
                    lcm, config, context.getThisComponentId() + "." + context.getThisTaskIndex());
            joiner.start();
        } catch(Exception ex){
            LOG.error("Fail bringing up quartz scheduler.", ex);
            throw new IllegalStateException(ex);
        }
    }

    protected abstract void executeWithEnrich(Tuple input, Map<K, T> cache);

    @Override
    public void execute(Tuple input) {
        Map map = (Map) ExternalDataCache
                    .getInstance()
                    .getJobResult(lcm.getClass());
        executeWithEnrich(input, map);
    }
}
