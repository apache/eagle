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

package org.apache.eagle.app.messaging;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import com.typesafe.config.Config;
import org.apache.eagle.log.base.taggedlog.TaggedLogAPIEntity;
import org.apache.eagle.log.entity.GenericServiceAPIResponseEntity;
import org.apache.eagle.service.client.IEagleServiceClient;
import org.apache.eagle.service.client.impl.EagleServiceClientImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;

public class EntityStreamPersist extends BaseRichBolt {
    private static final Logger LOG = LoggerFactory.getLogger(EntityStreamPersist.class);

    private final Config config;
    private IEagleServiceClient client;
    private OutputCollector collector;
    private int batchSize;
    private List<TaggedLogAPIEntity> entityBucket = new CopyOnWriteArrayList<>();

    public EntityStreamPersist(Config config) {
        this.config = config;
        this.batchSize = config.hasPath("service.batchSize") ? config.getInt("service.batchSize") : 1;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.client = new EagleServiceClientImpl(config);
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        List<? extends TaggedLogAPIEntity> entities = (List<? extends TaggedLogAPIEntity>) input.getValue(0);
        entityBucket.addAll(entities);

        if (entityBucket.size() < batchSize) {
            return;
        }

        try {
            GenericServiceAPIResponseEntity response = client.create(entityBucket);
            if (response.isSuccess()) {
                LOG.info("persist {} entities with starttime={}", entityBucket.size(), entityBucket.get(0).getTimestamp());
                collector.ack(input);
            } else {
                LOG.error("Service side error: {}", response.getException());
                collector.reportError(new IllegalStateException(response.getException()));
            }
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            collector.fail(input);
        }
        entityBucket.clear();
    }

    @Override
    public void cleanup() {
        try {
            this.client.getJerseyClient().destroy();
            this.client.close();
        } catch (IOException e) {
            LOG.error("Close client error: {}", e.getMessage(), e);
        } finally {
            super.cleanup();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
}
