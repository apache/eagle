/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "+License"+); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "+AS IS"+ BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.eagle.metadata.store.jdbc;

import org.apache.commons.collections.map.HashedMap;
import org.apache.eagle.app.service.ApplicationProviderService;
import org.apache.eagle.metadata.exceptions.EntityNotFoundException;
import org.apache.eagle.metadata.model.ApplicationDesc;
import org.apache.eagle.metadata.model.ApplicationEntity;
import org.apache.eagle.metadata.model.SiteEntity;
import org.apache.eagle.metadata.service.ApplicationEntityService;
import org.apache.eagle.metadata.service.SiteEntityService;
import org.junit.Assert;
import org.junit.Test;

import javax.inject.Inject;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Map;

public class ApplicationEntityServiceJDBCImplTest extends JDBCMetadataTestBase {

    @Inject
    ApplicationEntityService applicationEntityService;
    @Inject
    ApplicationProviderService applicationProviderService;
    @Inject
    SiteEntityService siteEntityService;

    @Test
    public void testCRUDApplicationEntity() throws SQLException, EntityNotFoundException {

        SiteEntity siteEntity = new SiteEntity();
        siteEntity.setSiteId("testsiteid");
        siteEntity.setSiteName("testsitename");
        siteEntity.setDescription("testdesc");
        siteEntityService.create(siteEntity);
        String siteuuid = siteEntity.getUuid();
        long sitecreateTime = siteEntity.getCreatedTime();
        long sitemodifiedTime = siteEntity.getModifiedTime();
        ApplicationDesc applicationDesc = applicationProviderService.getApplicationDescByType("TEST_APP");

        ApplicationEntity applicationEntity = new ApplicationEntity();
        applicationEntity.setSite(siteEntity);
        applicationEntity.setDescriptor(applicationDesc);
        applicationEntity.setMode(ApplicationEntity.Mode.LOCAL);
        applicationEntity.setJarPath(applicationDesc.getJarPath());
        Map<String, Object> configure = new HashedMap();
        configure.put("a", "b");
        applicationEntity.setConfiguration(configure);
        applicationEntity.setContext(configure);
        applicationEntityService.create(applicationEntity);
        String appuuid = applicationEntity.getUuid();
        String appId = applicationEntity.getAppId();
        long createTime = applicationEntity.getCreatedTime();
        long modifiedTime = applicationEntity.getModifiedTime();

        Collection<ApplicationEntity> results = applicationEntityService.findAll();
        Assert.assertEquals(1, results.size());
        ApplicationEntity applicationEntityFromDB = applicationEntityService.getBySiteIdAndAppType("testsiteid", "TEST_APP");
        Assert.assertTrue(applicationEntityFromDB != null);
        results = applicationEntityService.findBySiteId("testsiteid");
        Assert.assertEquals(1, results.size());

        applicationEntityFromDB = applicationEntityService.getByUUID(appuuid);

        Assert.assertTrue(applicationEntityFromDB != null);
        applicationEntityFromDB = applicationEntityService.getByUUIDOrAppId(null, appId);

        Assert.assertEquals(siteuuid, applicationEntityFromDB.getSite().getUuid());
        Assert.assertEquals("testsiteid", applicationEntityFromDB.getSite().getSiteId());
        Assert.assertEquals("testsitename", applicationEntityFromDB.getSite().getSiteName());
        Assert.assertEquals("testdesc", applicationEntityFromDB.getSite().getDescription());
        Assert.assertEquals(sitecreateTime, applicationEntityFromDB.getSite().getCreatedTime());
        Assert.assertEquals(sitemodifiedTime, applicationEntityFromDB.getSite().getModifiedTime());

        Assert.assertEquals(appuuid, applicationEntityFromDB.getUuid());
        Assert.assertEquals(appId, applicationEntityFromDB.getAppId());
        Assert.assertEquals("TEST_APP", applicationEntityFromDB.getDescriptor().getType());
        Assert.assertEquals(ApplicationEntity.Mode.LOCAL, applicationEntityFromDB.getMode());
        Assert.assertEquals(applicationDesc.getJarPath(), applicationEntityFromDB.getJarPath());
        Assert.assertEquals(ApplicationEntity.Status.INITIALIZED, applicationEntityFromDB.getStatus());
        Assert.assertEquals(createTime, applicationEntityFromDB.getCreatedTime());
        Assert.assertEquals(modifiedTime, applicationEntityFromDB.getModifiedTime());


    }

    @Test(expected = IllegalArgumentException.class)
    public void testInsertApplicationEntityFail() {
        SiteEntity siteEntity = new SiteEntity();
        siteEntity.setSiteId("testsiteid");
        siteEntity.setSiteName("testsitename");
        siteEntity.setDescription("testdesc");
        siteEntityService.create(siteEntity);
        ApplicationDesc applicationDesc = applicationProviderService.getApplicationDescByType("TEST_APP");

        ApplicationEntity applicationEntity = new ApplicationEntity();
        applicationEntity.setSite(siteEntity);
        applicationEntity.setDescriptor(applicationDesc);
        applicationEntity.setMode(ApplicationEntity.Mode.LOCAL);
        applicationEntity.setJarPath(applicationDesc.getJarPath());
        Map<String, Object> configure = new HashedMap();
        configure.put("a", "b");
        applicationEntity.setConfiguration(configure);
        applicationEntity.setContext(configure);
        applicationEntityService.create(applicationEntity);
        applicationEntityService.create(applicationEntity);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetByUUIDFail() throws EntityNotFoundException {
        applicationEntityService.getByUUID("fake uuid");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetByUUIDOrAppId() {
        applicationEntityService.getByUUIDOrAppId(null, "fake appid");
    }

    @Test
    public void testDelApplicationEntity() {
        SiteEntity siteEntity = new SiteEntity();
        siteEntity.setSiteId("testsiteid");
        siteEntity.setSiteName("testsitename");
        siteEntity.setDescription("testdesc");
        siteEntityService.create(siteEntity);
        ApplicationDesc applicationDesc = applicationProviderService.getApplicationDescByType("TEST_APP");


        ApplicationEntity applicationEntity = new ApplicationEntity();
        applicationEntity.setSite(siteEntity);
        applicationEntity.setDescriptor(applicationDesc);
        applicationEntity.setMode(ApplicationEntity.Mode.LOCAL);
        applicationEntity.setJarPath(applicationDesc.getJarPath());
        Map<String, Object> configure = new HashedMap();
        configure.put("a", "b");
        applicationEntity.setConfiguration(configure);
        applicationEntity.setContext(configure);
        applicationEntityService.create(applicationEntity);
        Collection<ApplicationEntity> results = applicationEntityService.findAll();
        Assert.assertEquals(1, results.size());
        applicationEntityService.delete(applicationEntity);
        results = applicationEntityService.findAll();
        Assert.assertEquals(0, results.size());
    }

}
