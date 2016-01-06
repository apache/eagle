/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *    http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.eagle.state.snapshot;

import com.typesafe.config.Config;
import org.apache.eagle.state.base.Snapshotable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * service for periodically taking snapshots and persisting snapshots to durable storage
 */
public class StateSnapshotService {
    private final static Logger LOG = LoggerFactory.getLogger(StateSnapshotService.class);

    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

    public StateSnapshotService(Config config, final Snapshotable snapshotable, final StateSnapshotDAO stateDAO, final Object snapshotLock, final AtomicBoolean shouldPersist){
        // start up daemon thread to periodically take snapshot
        long interval = config.getLong("eagleProps.executorState.snapshotIntervalMS");
        final String site = config.getString("eagleProps.site");
        final String applicationId = config.getString("eagleProps.dataSource");
        final String executorId = snapshotable.getElementId();
        scheduler.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                LOG.info("start taking state snapshot for " + applicationId + "/" + executorId);
                synchronized (snapshotLock) {
                    try {
                        byte[] state = snapshotable.currentState();
                        stateDAO.writeState(site, applicationId, executorId, state);
                        shouldPersist.set(true);
                    }catch(Exception ex){
                        LOG.error("fail writing state, but continue to run", ex);
                    }
                }
                LOG.info("end taking state snapshot for " + applicationId + "/" + executorId);
            }
        }, 0L, interval, TimeUnit.MILLISECONDS);
    }
}
