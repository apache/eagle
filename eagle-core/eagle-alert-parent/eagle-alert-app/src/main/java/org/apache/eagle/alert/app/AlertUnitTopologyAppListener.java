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

package org.apache.eagle.alert.app;

import org.apache.eagle.alert.coordination.model.internal.Topology;
import org.apache.eagle.alert.engine.runner.UnitTopologyRunner;
import org.apache.eagle.alert.metadata.IMetadataDao;
import org.apache.eagle.alert.metadata.resource.OpResult;
import org.apache.eagle.app.service.ApplicationListener;
import org.apache.eagle.metadata.model.ApplicationEntity;
import com.typesafe.config.ConfigFactory;

import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class AlertUnitTopologyAppListener implements ApplicationListener {
    private static final Logger LOG = LoggerFactory.getLogger(AlertUnitTopologyAppListener.class);

    @Inject private IMetadataDao metadataDao;

    private ApplicationEntity applicationEntity;

    @Override
    public void init(ApplicationEntity applicationEntity) {
        this.applicationEntity = applicationEntity;
    }

    @Override
    public void afterInstall() {
        // Do nothing
    }

    @Override
    public void afterUninstall() {
        removeTopologyMetadata();
    }

    @Override
    public void beforeStart() {
        // Do thing, may do some validation works?
        updateTopologyMetadata();
    }

    @Override
    public void afterStop() {
        removeTopologyMetadata();
    }

    // -------------
    // Internal RPC
    // -------------

    private void updateTopologyMetadata() {
        LOG.info("Update topology metadata {}", this.applicationEntity.getAppId());
        OpResult result = metadataDao.addTopology(createTopologyMeta(this.applicationEntity));
        if (result.code == OpResult.FAILURE) {
            LOG.error(result.message);
            throw new IllegalStateException(result.message);
        }
    }

    private void removeTopologyMetadata() {
        LOG.info("Remove topology metadata {}", this.applicationEntity.getAppId());
        OpResult result = metadataDao.removeTopology(createTopologyMeta(this.applicationEntity).getName());
        if (result.code == OpResult.FAILURE) {
            LOG.error(result.message);
            throw new IllegalStateException(result.message);
        }
    }

    private Topology createTopologyMeta(ApplicationEntity applicationEntity) {
        return UnitTopologyRunner.buildTopologyMetadata(applicationEntity.getAppId(),ConfigFactory.parseMap(applicationEntity.getConfiguration()));
    }
}