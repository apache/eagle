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

package org.apache.eagle.jpm.mr.running.parser;

import org.apache.eagle.jpm.mr.running.config.MRRunningConfigManager;
import org.apache.eagle.jpm.util.Utils;
import org.apache.eagle.log.base.taggedlog.TaggedLogAPIEntity;
import org.apache.eagle.service.client.IEagleServiceClient;
import org.apache.eagle.service.client.impl.EagleServiceClientImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;

public class MRJobEntityCreationHandler {
    private static final Logger LOG = LoggerFactory.getLogger(MRJobEntityCreationHandler.class);

    class EntityFlushThread extends Thread {
        private final Logger LOG = LoggerFactory.getLogger(EntityFlushThread.class);
        private Object entityLock = new Object();
        private Deque<List<TaggedLogAPIEntity>> listDeque = new LinkedList<>();
        private MRRunningConfigManager.EagleServiceConfig eagleServiceConfig;

        public EntityFlushThread(MRRunningConfigManager.EagleServiceConfig eagleServiceConfig) {
            this.eagleServiceConfig = eagleServiceConfig;
        }

        public void enqueue(List<TaggedLogAPIEntity> entities) {
            synchronized (entityLock) {
                listDeque.add(entities);
            }
        }

        public boolean flush(List<TaggedLogAPIEntity> entities) {
            //need flush right now
            try {
                IEagleServiceClient client = new EagleServiceClientImpl(
                        eagleServiceConfig.eagleServiceHost,
                        eagleServiceConfig.eagleServicePort,
                        eagleServiceConfig.username,
                        eagleServiceConfig.password);

                LOG.info("start to flush mr job entities, size {}", entities.size());
                client.create(entities);
                LOG.info("finish flushing mr job entities, size {}", entities.size());
                entities.clear();
            } catch (Exception e) {
                LOG.warn("exception found when flush entities, {}", e);
                e.printStackTrace();
                return false;
            }

            return true;
        }

        @Override
        public void run() {
            while (true) {
                List<TaggedLogAPIEntity> entities = null;
                synchronized (entityLock) {
                    if (!listDeque.isEmpty()) {
                        entities = listDeque.pollFirst();
                    }
                }

                if (entities != null) {
                    flush(entities);
                }
                Utils.sleep(1);
            }
        }
    }

    private EntityFlushThread entityFlushThread;
    private List<TaggedLogAPIEntity> entities = new ArrayList<>();
    private static final int MAX_ENTITIES_SIZE = 1000;

    private final Object lock = new Object();
    public MRJobEntityCreationHandler(MRRunningConfigManager.EagleServiceConfig eagleServiceConfig) {
        this.entityFlushThread = new EntityFlushThread(eagleServiceConfig);
        this.entityFlushThread.start();
    }

    public boolean add(TaggedLogAPIEntity entity) {
        synchronized (lock) {
            if (entity != null) {
                entities.add(entity);
            }

            if (entity == null) { //force to flush
                return this.entityFlushThread.flush(entities);
            }

            //flush in another thread
            if (entities.size() >= MAX_ENTITIES_SIZE) {
                this.entityFlushThread.enqueue(entities);
                entities = new ArrayList<>();
            }
        }

        return true;
    }
}
