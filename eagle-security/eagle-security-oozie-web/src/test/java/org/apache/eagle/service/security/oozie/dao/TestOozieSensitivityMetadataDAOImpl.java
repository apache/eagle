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


import org.apache.eagle.log.entity.GenericServiceAPIResponseEntity;
import org.apache.eagle.security.entity.OozieResourceSensitivityAPIEntity;
import org.apache.eagle.service.generic.GenericEntityServiceResource;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.easymock.EasyMock.expect;
import static org.powermock.api.easymock.PowerMock.*;


@RunWith(PowerMockRunner.class)
@PrepareForTest(OozieSensitivityMetadataDAOImpl.class)
public class TestOozieSensitivityMetadataDAOImpl {
    @Test
    public void testGetAllOozieSensitivityMap() throws Exception {

        String[] sites = new String[]{"test", "test"};
        mockGenericEntityServiceResourceSearchMethod("OozieResourceSensitivityService[]{*}",sites);
        OozieSensitivityMetadataDAO oozieSensitivityMetadataDAO = new OozieSensitivityMetadataDAOImpl();
        Map<String, Map<String, String>> allOozieSensitivityMap = oozieSensitivityMetadataDAO.getAllOozieSensitivityMap();
        Assert.assertEquals(1, allOozieSensitivityMap.size());
        Assert.assertTrue(allOozieSensitivityMap.containsKey("test"));
        Assert.assertTrue(allOozieSensitivityMap.get("test").containsKey("co_pms_ods_adv_click_log"));
        Assert.assertEquals("pms job", allOozieSensitivityMap.get("test").get("co_pms_ods_adv_click_log"));
        Assert.assertTrue(allOozieSensitivityMap.get("test").containsKey("co_tandem_ods_adv_click_log"));
        Assert.assertEquals("tandem job", allOozieSensitivityMap.get("test").get("co_tandem_ods_adv_click_log"));

    }

    @Test
    public void testGetAllOozieSensitivityMapWithDiffSite() throws Exception {

        String[] sites = new String[]{"test", "test1"};
        mockGenericEntityServiceResourceSearchMethod("OozieResourceSensitivityService[]{*}",sites);
        OozieSensitivityMetadataDAO oozieSensitivityMetadataDAO = new OozieSensitivityMetadataDAOImpl();
        Map<String, Map<String, String>> allOozieSensitivityMap = oozieSensitivityMetadataDAO.getAllOozieSensitivityMap();
        Assert.assertEquals(2, allOozieSensitivityMap.size());
        Assert.assertTrue(allOozieSensitivityMap.containsKey("test"));
        Assert.assertTrue(allOozieSensitivityMap.containsKey("test1"));
        Assert.assertTrue(allOozieSensitivityMap.get("test1").containsKey("co_pms_ods_adv_click_log"));
        Assert.assertEquals("pms job", allOozieSensitivityMap.get("test1").get("co_pms_ods_adv_click_log"));
        Assert.assertTrue(allOozieSensitivityMap.get("test").containsKey("co_tandem_ods_adv_click_log"));
        Assert.assertEquals("tandem job", allOozieSensitivityMap.get("test").get("co_tandem_ods_adv_click_log"));

    }

    @Test
    public void testGetOozieSensitivityMap() throws Exception {

        String[] sites = new String[]{"test", "test"};
        mockGenericEntityServiceResourceSearchMethod("OozieResourceSensitivityService[@site=\"test\"]{*}",sites);
        OozieSensitivityMetadataDAO oozieSensitivityMetadataDAO = new OozieSensitivityMetadataDAOImpl();
        Map<String, String> oozieSensitivityMap = oozieSensitivityMetadataDAO.getOozieSensitivityMap("test");
        Assert.assertEquals(2, oozieSensitivityMap.size());
        Assert.assertTrue(oozieSensitivityMap.containsKey("co_pms_ods_adv_click_log"));
        Assert.assertTrue(oozieSensitivityMap.containsKey("co_tandem_ods_adv_click_log"));
        Assert.assertEquals("pms job", oozieSensitivityMap.get("co_pms_ods_adv_click_log"));
        Assert.assertEquals("tandem job", oozieSensitivityMap.get("co_tandem_ods_adv_click_log"));

    }

    private void mockGenericEntityServiceResourceSearchMethod(String queryStr ,String[] sites) throws Exception {
        GenericEntityServiceResource genericEntityServiceResourceMock = createMock(GenericEntityServiceResource.class);
        expectNew(GenericEntityServiceResource.class).andReturn(genericEntityServiceResourceMock);
        GenericServiceAPIResponseEntity ret = new GenericServiceAPIResponseEntity();
        List<OozieResourceSensitivityAPIEntity> entities = getOozieResourceSensitivityAPIEntities(sites);
        ret.setObj(entities);
        expect(genericEntityServiceResourceMock.search(queryStr, null, null, Integer.MAX_VALUE, null, false, false, 0L, 0, false,
                0, null, false)).andReturn(ret);

        replay(genericEntityServiceResourceMock, GenericEntityServiceResource.class);
    }

    private List<OozieResourceSensitivityAPIEntity> getOozieResourceSensitivityAPIEntities(final String[] sites) {
        List<OozieResourceSensitivityAPIEntity> entities = new ArrayList<OozieResourceSensitivityAPIEntity>();
        OozieResourceSensitivityAPIEntity entity1 = new OozieResourceSensitivityAPIEntity();
        entity1.setTags(new HashMap<String, String>() {{
            put("site", sites[0]);
            put("oozieResource", "co_tandem_ods_adv_click_log");
        }});
        entity1.setSensitivityType("tandem job");
        OozieResourceSensitivityAPIEntity entity2 = new OozieResourceSensitivityAPIEntity();
        entity2.setTags(new HashMap<String, String>() {{
            put("site", sites[1]);
            put("oozieResource", "co_pms_ods_adv_click_log");
        }});
        entity2.setSensitivityType("pms job");
        entities.add(entity1);
        entities.add(entity2);
        return entities;
    }

}
