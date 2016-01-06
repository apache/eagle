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
import org.apache.eagle.state.ExecutorStateConstants;
import org.apache.eagle.state.entity.ExecutorStateSnapshotEntity;
import org.apache.eagle.log.entity.GenericServiceAPIResponseEntity;
import org.apache.eagle.service.client.EagleServiceConnector;
import org.apache.eagle.service.client.IEagleServiceClient;
import org.apache.eagle.service.client.impl.EagleServiceClientImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

/**
 * DAO methods backed by eagle service
 */
public class StateSnapshotEagleServiceDAOImpl implements StateSnapshotDAO{
    private final static Logger LOG = LoggerFactory.getLogger(StateSnapshotEagleServiceDAOImpl.class);

    private Config config;
    public StateSnapshotEagleServiceDAOImpl(Config config){
        this.config = config;
    }
    @Override
    public void writeState(final String site, final String applicationId, final String executorId, byte[] state) throws IOException{
        LOG.info("start writing state snapshot: " + applicationId + "/" + executorId + ", with state byte size " + state.length);
        ExecutorStateSnapshotEntity entity = new ExecutorStateSnapshotEntity();
        entity.setTags(new HashMap<String, String>(){{
            put("site", site);
            put("applicationId", applicationId);
            put("executorId", executorId);
        }});
        entity.setTimestamp(new Date().getTime());
        entity.setState(state);
        IEagleServiceClient client = new EagleServiceClientImpl(new EagleServiceConnector(config));
        GenericServiceAPIResponseEntity response = null;
        try{
            response = client.create(Arrays.asList(entity));
        }catch(Exception ex){
            LOG.error("fail creating entity", ex);
            throw new IOException(ex);
        }
        if(!response.isSuccess()){
            LOG.error("fail creating entity with exception " + response.getException());
            throw new IOException(response.getException());
        }
        client.close();
        LOG.info("end writing state snapshot: " + applicationId + "/" + executorId);
    }

    @Override
    public ExecutorStateSnapshotEntity findLatestState(String site, String applicationId, String executorId) throws IOException{
        IEagleServiceClient client = new EagleServiceClientImpl(new EagleServiceConnector(config));
        String query = ExecutorStateConstants.POLICY_STATE_SNAPSHOT_SERVICE_ENDPOINT_NAME + "[@applicationId=\"" + applicationId +
                "\" AND @site=\"" + site +
                "\" AND @executorId=\"" + executorId +
                "\"]{*}";
        GenericServiceAPIResponseEntity<ExecutorStateSnapshotEntity> response = null;
        try {
            response = client.search()
                    .startTime(0)
                    .endTime(new Date().getTime())
                    .pageSize(1)
                    .query(query)
                    .send();
        }catch(Exception ex){
            LOG.error("error querying state snapshot" + ex);
            throw new IOException(ex);
        }
        client.close();
        if (response.getException() != null) {
            throw new IOException("Got an exception when query eagle service: " + response.getException());
        }
        List<ExecutorStateSnapshotEntity> entities = response.getObj();
        if(entities.size() >= 1){
            return entities.get(0);
        }
        return null;
    }
}
