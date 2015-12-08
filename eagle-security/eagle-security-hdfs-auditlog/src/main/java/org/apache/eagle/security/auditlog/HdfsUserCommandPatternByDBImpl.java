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
package org.apache.eagle.security.auditlog;

import org.apache.eagle.log.entity.GenericServiceAPIResponseEntity;
import org.apache.eagle.security.hdfs.entity.HdfsUserCommandPatternEntity;
import org.apache.eagle.service.client.EagleServiceConnector;
import org.apache.eagle.service.client.IEagleServiceClient;
import org.apache.eagle.service.client.impl.EagleServiceClientImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * get HdfsUserCommandPattern from eagle database
 */
public class HdfsUserCommandPatternByDBImpl implements HdfsUserCommandPatternDAO {
    private final EagleServiceConnector connector;
    private final Logger LOG = LoggerFactory.getLogger(HdfsUserCommandPatternByDBImpl.class);

    @Override
    public List<HdfsUserCommandPatternEntity> findAllPatterns() throws Exception{
        try {
            IEagleServiceClient client = new EagleServiceClientImpl(this.connector);
            String query = HdfsUserCommandPatternEntity.HDFS_USER_COMMAND_PATTERN_SERVICE + "[]{*}";
            GenericServiceAPIResponseEntity<HdfsUserCommandPatternEntity> response =  client.search()
                    .pageSize(Integer.MAX_VALUE)
                    .query(query)
                    .send();
            client.close();
            if (response.getException() != null) {
                throw new Exception("Got an exception when query eagle service: " + response.getException());
            }
            List<HdfsUserCommandPatternEntity> list = response.getObj();
            return list;
        }
        catch (Exception ex) {
            LOG.error("Got an exception when query HdfsUserCommandPattern service", ex);
            throw ex;
        }
    }

    public HdfsUserCommandPatternByDBImpl(EagleServiceConnector connector) {
        this.connector = connector;
    }
}
