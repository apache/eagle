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
package org.apache.eagle.service.security.oozie;

import org.apache.eagle.security.entity.OozieResourceEntity;
import org.apache.eagle.service.security.oozie.dao.OozieSensitivityMetadataDAO;
import org.apache.eagle.service.security.oozie.dao.OozieSensitivityMetadataDAOImpl;
import org.apache.oozie.client.CoordinatorJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class OozieResourceSensitivityDataJoiner {

    private static final Logger LOG = LoggerFactory.getLogger(OozieResourceSensitivityDataJoiner.class);

    public List<OozieResourceEntity> joinOozieResourceSensitivity(String site, List<CoordinatorJob> coordinatorJobs) {

        OozieSensitivityMetadataDAO oozieSensitivityMetadataDAO = new OozieSensitivityMetadataDAOImpl();
        Map<String, String> sensitivityMap = oozieSensitivityMetadataDAO.getOozieSensitivityMap(site);
        LOG.info("Joining Resource with Sensitivity data ..");
        List<OozieResourceEntity> result = new ArrayList<>();
        for (CoordinatorJob eachJob : coordinatorJobs) {
            OozieResourceEntity entity = new OozieResourceEntity(eachJob.getId(), eachJob.getAppName(), sensitivityMap.get(eachJob.getId()));
            result.add(entity);
        }
        return result;
    }

}