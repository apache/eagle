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

package org.apache.eagle.state;

import com.typesafe.config.Config;
import org.apache.eagle.state.deltaevent.DeltaEventDAO;
import org.apache.eagle.state.deltaevent.DeltaEventReplayCallback;
import org.apache.eagle.state.deltaeventoffset.DeltaEventOffsetRangeDAO;
import org.apache.eagle.state.entity.DeltaEventOffsetRangeEntity;
import org.apache.eagle.state.entity.ExecutorStateSnapshotEntity;
import org.apache.eagle.state.base.Snapshotable;
import org.apache.eagle.state.snapshot.StateSnapshotDAO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * State recovery includes two steps
 * 1. apply latest snapshot
 * 2. apply all events since latest snapshot
 */
public class StateRecoveryService {
    private final static Logger LOG = LoggerFactory.getLogger(StateRecoveryService.class);

    private Config config;
    private Snapshotable snapshotable;
    private StateSnapshotDAO stateSnapshotDAO;
    private DeltaEventDAO deltaEventDAO;
    private DeltaEventOffsetRangeDAO deltaEventOffsetRangeDAO;
    private DeltaEventReplayCallback callback;
    public StateRecoveryService(Config config,
                                Snapshotable snapshotable,
                                StateSnapshotDAO stateSnapshotDAO,
                                DeltaEventDAO deltaEventDAO,
                                DeltaEventOffsetRangeDAO deltaEventOffsetRangeDAO,
                                DeltaEventReplayCallback callback){
        this.config = config;
        this.snapshotable = snapshotable;
        this.stateSnapshotDAO = stateSnapshotDAO;
        this.deltaEventDAO = deltaEventDAO;
        this.deltaEventOffsetRangeDAO = deltaEventOffsetRangeDAO;
        this.callback = callback;
    }

    public void recover(){
        String site = config.getString("eagleProps.site");
        String applicationId = config.getString("eagleProps.dataSource");
        String elementId = snapshotable.getElementId();
        LOG.info("start recovering state for " + applicationId + "/" + elementId);
        LOG.info("step 1 of 2, start restoring snapshot for " + applicationId + "/" + elementId);
        // fetch state from DAO
        ExecutorStateSnapshotEntity snapshotEntity = null;
        try{
            snapshotEntity = stateSnapshotDAO.findLatestState(site, applicationId, elementId);
        }catch(Exception ex){
            LOG.error("error finding latest state snapshot, but continue to run", ex);
            return;
        }
        byte[] snapshot = null;
        if(snapshotEntity != null) {
            snapshot = snapshotEntity.getState();
        }else{
            LOG.warn("snapshot is empty, continue to run");
        }
        snapshotable.restoreState(snapshot);
        LOG.info("step 1 of 2, end restoring snapshot for " + applicationId + "/" + elementId);
        // apply delta devents
        LOG.info("step 2 of 2, start replaying delta events for " + applicationId + "/" + elementId);
        // 1. get starting id
        DeltaEventOffsetRangeEntity idRangeEntity = null;
        try{
            idRangeEntity = deltaEventOffsetRangeDAO.findLatestIdRange();
        }catch(Exception ex){
            LOG.error("fail reading deltaEventIdRange, use max value");
        }
        if(idRangeEntity == null){
            LOG.warn("delta event is empty, so ignore event replay");
        }else if(idRangeEntity.getTimestamp() < snapshotEntity.getTimestamp()){
            LOG.warn("snapshot is newer than last recorded delta event, " + snapshotEntity.getTimestamp() + ">" + idRangeEntity.getTimestamp() + ", so ignore event replay");
        }else {
            long startingOffset = idRangeEntity.getStartingOffset();
            LOG.info("recorded delta event startingOffset " + startingOffset);
            // 2. get max offset right now
            try {
                deltaEventDAO.load(startingOffset, callback);
            } catch (Exception ex) {
                LOG.error("fail loading deltaEvent starting offset, but continue to run", ex);
            }
        }
        LOG.info("step 2 of 2, end replaying delta events for " + applicationId + "/" + elementId);
        LOG.info("end recovering state for " + applicationId + "/" + elementId);
    }
}
