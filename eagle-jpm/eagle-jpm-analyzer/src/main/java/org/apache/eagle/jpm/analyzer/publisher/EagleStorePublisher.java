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

package org.apache.eagle.jpm.analyzer.publisher;

import com.typesafe.config.Config;
import org.apache.eagle.jpm.analyzer.meta.model.AnalyzerEntity;
import org.apache.eagle.jpm.analyzer.publisher.dedup.AlertDeduplicator;
import org.apache.eagle.jpm.analyzer.publisher.dedup.impl.SimpleDeduplicator;
import org.apache.eagle.log.base.taggedlog.TaggedLogAPIEntity;
import org.apache.eagle.service.client.IEagleServiceClient;
import org.apache.eagle.service.client.impl.EagleServiceClientImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Map;

public class EagleStorePublisher implements Publisher, Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(EagleStorePublisher.class);

    private Config config;
    private IEagleServiceClient client;

    public EagleStorePublisher(Config config) {
        this.config = config;
    }

    @Override
    public void publish(AnalyzerEntity analyzerJobEntity, Result result) {
        if (result.getAlertEntities().size() == 0) {
            return;
        }

        LOG.info("EagleStorePublisher gets job {}", analyzerJobEntity.getJobId());

        try {
            this.client = new EagleServiceClientImpl(config);
            for (Map.Entry<String, List<TaggedLogAPIEntity>> entry : result.getAlertEntities().entrySet()) {
                client.create(entry.getValue());
                LOG.info("successfully persist {} entities for evaluator {}", entry.getValue().size(), entry.getKey());
            }
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        } finally {
            if (client != null) {
                try {
                    client.close();
                } catch (IOException e) {
                    LOG.error(e.getMessage(), e);
                }
            }
        }

    }
}
