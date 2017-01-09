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

package org.apache.eagle.jpm.analyzer.mr.meta.impl;

import com.google.inject.Inject;
import com.typesafe.config.Config;
import org.apache.eagle.jpm.analyzer.mr.meta.MetaManagementService;
import org.apache.eagle.jpm.analyzer.JobMetaEntity;
import org.apache.eagle.jpm.analyzer.mr.meta.model.PublisherEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.List;

public class MetaManagementServiceJDBCImpl implements MetaManagementService, Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(MetaManagementServiceJDBCImpl.class);

    @Inject
    Config config;

    @Override
    public boolean addJobMeta(JobMetaEntity jobMetaEntity) {

        return true;
    }

    @Override
    public boolean updateJobMeta(String jobDefId, JobMetaEntity jobMetaEntity) {

        return true;
    }

    @Override
    public List<JobMetaEntity> getJobMeta(String jobDefId) {

        return null;
    }

    @Override
    public boolean deleteJobMeta(String jobDefId) {

        return true;
    }

    @Override
    public boolean addPublisherMeta(PublisherEntity publisherEntity) {

        return true;
    }

    @Override
    public boolean deletePublisherMeta(String userId) {

        return true;
    }

    @Override
    public List<PublisherEntity> getPublisherMeta(String userId) {
        return null;
    }
}
