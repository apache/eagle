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
import org.apache.eagle.service.security.oozie.dao.OozieSensitivityMetadataDAOImpl;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.rest.JsonTags;
import org.apache.oozie.client.rest.JsonToBean;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.easymock.EasyMock.expect;
import static org.powermock.api.easymock.PowerMock.*;

@RunWith(PowerMockRunner.class)
@PrepareForTest(OozieResourceSensitivityDataJoiner.class)
public class TestOozieResourceSensitivityDataJoiner {
    @Test
    public void testOozieResourceSensitivityDataJoiner() throws Exception {

        List<CoordinatorJob> coordinatorJobs = getCoordinatorJobs();
        mockGetOozieSensitivityMap();
        OozieResourceSensitivityDataJoiner joiner = new OozieResourceSensitivityDataJoiner();
        List<OozieResourceEntity> oozieResourceEntitys = joiner.joinOozieResourceSensitivity("test", coordinatorJobs);

        Assert.assertEquals(3, oozieResourceEntitys.size());
        Assert.assertEquals("0007197-160509174709457-oozie-oozi-C", oozieResourceEntitys.get(0).getJobId());
        Assert.assertEquals("co_pms_ods_adv_click_log", oozieResourceEntitys.get(0).getName());
        Assert.assertEquals("pms job", oozieResourceEntitys.get(0).getSensitiveType());

        Assert.assertEquals("0007189-160509174709457-oozie-oozi-C", oozieResourceEntitys.get(1).getJobId());
        Assert.assertEquals("co_tandem_ods_adv_click_log", oozieResourceEntitys.get(1).getName());
        Assert.assertEquals("tandem job", oozieResourceEntitys.get(1).getSensitiveType());

        Assert.assertEquals("0007188-160509174709457-oozie-oozi-C", oozieResourceEntitys.get(2).getJobId());
        Assert.assertEquals("co_cpc_new_custorm", oozieResourceEntitys.get(2).getName());
        Assert.assertEquals(null, oozieResourceEntitys.get(2).getSensitiveType());

    }

    private List<CoordinatorJob> getCoordinatorJobs() {
        List<CoordinatorJob> coordinatorJobs;
        InputStream jsonstream = this.getClass().getResourceAsStream("/coordinatorJob.json");
        JSONObject json = (JSONObject) JSONValue.parse(new InputStreamReader(jsonstream));
        JSONArray jobs = (JSONArray) json.get(JsonTags.COORDINATOR_JOBS);
        coordinatorJobs = JsonToBean.createCoordinatorJobList(jobs);
        return coordinatorJobs;
    }

    private void mockGetOozieSensitivityMap() throws Exception {
        OozieSensitivityMetadataDAOImpl oozieSensitivityMetadataDAOMock = createMock(OozieSensitivityMetadataDAOImpl.class);
        expectNew(OozieSensitivityMetadataDAOImpl.class).andReturn(oozieSensitivityMetadataDAOMock);
        Map<String, String> oozieSensitivityMap = new HashMap<String, String>();
        oozieSensitivityMap.put("0007197-160509174709457-oozie-oozi-C", "pms job");
        oozieSensitivityMap.put("0007189-160509174709457-oozie-oozi-C", "tandem job");
        expect(oozieSensitivityMetadataDAOMock.getOozieSensitivityMap("test")).andReturn(oozieSensitivityMap);
        replay(oozieSensitivityMetadataDAOMock, OozieSensitivityMetadataDAOImpl.class);
    }
}
