/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

package org.apache.eagle.jpm.spark.running.parser;

import org.apache.eagle.jpm.spark.running.common.SparkRunningConfigManager;
import org.apache.eagle.log.base.taggedlog.TaggedLogAPIEntity;
import org.apache.eagle.service.client.IEagleServiceClient;
import org.apache.eagle.service.client.impl.EagleServiceClientImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class SparkAppEntityCreationHandler {
    private static final Logger LOG = LoggerFactory.getLogger(SparkAppEntityCreationHandler.class);

    private List<TaggedLogAPIEntity> entities = new ArrayList<>();
    private SparkRunningConfigManager.EagleServiceConfig eagleServiceConfig;

    public SparkAppEntityCreationHandler(SparkRunningConfigManager.EagleServiceConfig eagleServiceConfig) {
        this.eagleServiceConfig = eagleServiceConfig;
    }

    public void add(TaggedLogAPIEntity entity) {
        entities.add(entity);
        if (entities.size() >= eagleServiceConfig.maxFlushNum) {
            this.flush();
        }
    }

    public boolean flush() {
        //need flush right now
        if (entities.size() == 0) {
            return true;
        }
        IEagleServiceClient client = new EagleServiceClientImpl(
                eagleServiceConfig.eagleServiceHost,
                eagleServiceConfig.eagleServicePort,
                eagleServiceConfig.username,
                eagleServiceConfig.password);
        client.getJerseyClient().setReadTimeout(eagleServiceConfig.readTimeoutSeconds * 1000);
        try {
            LOG.info("start to flush spark app entities, size {}", entities.size());
            client.create(entities);
            LOG.info("finish flushing spark app entities, size {}", entities.size());
            entities.clear();
        } catch (Exception e) {
            LOG.warn("exception found when flush entities, {}", e);
            e.printStackTrace();
            return false;
        } finally {
            client.getJerseyClient().destroy();
            try {
                client.close();
            } catch (Exception e) {
            }
        }

        return true;
    }
}
