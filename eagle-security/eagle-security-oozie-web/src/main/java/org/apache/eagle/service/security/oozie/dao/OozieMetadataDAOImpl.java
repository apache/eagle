/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.eagle.service.security.oozie.dao;

import org.apache.oozie.client.AuthOozieClient;
import org.apache.oozie.client.CoordinatorJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;

public class OozieMetadataDAOImpl implements OozieMetadataDAO {


    private static Logger LOG = LoggerFactory.getLogger(OozieMetadataDAOImpl.class);

    private OozieMetadataAccessConfig config;

    public OozieMetadataDAOImpl(OozieMetadataAccessConfig config) {
        if (config.getAccessType() == null) {
            throw new BadOozieMetadataAccessConfigException("access Type is null, options: [oozie_api]");
        }
        this.config = config;
    }

    private AuthOozieClient createOozieConnection() throws Exception {
        AuthOozieClient client = new AuthOozieClient(config.getOozieUrl(), config.getAuthType());
        return client;
    }

    @Override
    public List<CoordinatorJob> getCoordJobs() throws Exception {
        AuthOozieClient client = createOozieConnection();
        List<CoordinatorJob> jobs = client.getCoordJobsInfo(config.getFilter(), 0, Integer.MAX_VALUE);
        if (jobs == null || jobs.isEmpty()) {
            return Collections.emptyList();
        }
        return jobs;
    }
}
