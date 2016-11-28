/**
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
package org.apache.eagle.metadata.service;

import junit.framework.Assert;
import org.apache.commons.collections.map.HashedMap;
import org.apache.eagle.metadata.exceptions.EntityNotFoundException;
import org.apache.eagle.metadata.model.ApplicationDesc;
import org.apache.eagle.metadata.model.ApplicationEntity;
import org.apache.eagle.metadata.model.SiteEntity;
import org.apache.eagle.metadata.service.memory.ApplicationEntityServiceMemoryImpl;
import org.junit.Test;
import org.mockito.Mockito;
import java.util.Collection;
import java.util.Map;

/**
 * @Since 11/23/16.
 */
public class TestApplicationEntityServiceMemoryImpl {
    ApplicationDescService applicationDescService = Mockito.mock(ApplicationDescService.class);
    ApplicationEntityService applicationEntityService = new ApplicationEntityServiceMemoryImpl(applicationDescService);

    @Test
    public void testCreate() {
        SiteEntity siteEntity = new SiteEntity();
        siteEntity.setSiteId("testsiteid");
        siteEntity.setSiteName("testsitename");
        siteEntity.setDescription("testdesc");
        ApplicationDesc applicationDesc = new ApplicationDesc();
        applicationDesc.setType("type1");
        ApplicationEntity applicationEntity = new ApplicationEntity();
        applicationEntity.setSite(siteEntity);
        applicationEntity.setDescriptor(applicationDesc);
        applicationEntity.setMode(ApplicationEntity.Mode.LOCAL);
        applicationEntity.setJarPath(applicationDesc.getJarPath());
        Map<String, Object> configure = new HashedMap();
        configure.put("a", "b");
        applicationEntity.setConfiguration(configure);
        applicationEntity.setContext(configure);
        ApplicationEntity entity = applicationEntityService.create(applicationEntity);
        System.out.println(entity.getUuid());
        Assert.assertNotNull(entity);
    }

    @Test
    public void testFindByUUID() throws EntityNotFoundException {
        SiteEntity siteEntity = new SiteEntity();
        siteEntity.setSiteId("testsiteid");
        siteEntity.setSiteName("testsitename");
        siteEntity.setDescription("testdesc");
        ApplicationDesc applicationDesc = new ApplicationDesc();
        applicationDesc.setType("type1");
        ApplicationEntity applicationEntity = new ApplicationEntity();
        applicationEntity.setSite(siteEntity);
        applicationEntity.setDescriptor(applicationDesc);
        applicationEntity.setMode(ApplicationEntity.Mode.LOCAL);
        applicationEntity.setJarPath(applicationDesc.getJarPath());
        Map<String, Object> configure = new HashedMap();
        configure.put("a", "b");
        applicationEntity.setConfiguration(configure);
        applicationEntity.setContext(configure);
        ApplicationEntity entity = applicationEntityService.create(applicationEntity);
        String uuid = entity.getUuid();
        ApplicationEntity resultEntity = applicationEntityService.getByUUID(uuid);
        Assert.assertNotNull(resultEntity);
    }

    @Test
    public void testFindBySiteId() throws EntityNotFoundException {
        SiteEntity siteEntity = new SiteEntity();
        siteEntity.setSiteId("testsiteid");
        siteEntity.setSiteName("testsitename");
        siteEntity.setDescription("testdesc");
        ApplicationDesc applicationDesc = new ApplicationDesc();
        applicationDesc.setType("type1");
        ApplicationEntity applicationEntity = new ApplicationEntity();
        applicationEntity.setSite(siteEntity);
        applicationEntity.setDescriptor(applicationDesc);
        applicationEntity.setMode(ApplicationEntity.Mode.LOCAL);
        applicationEntity.setJarPath(applicationDesc.getJarPath());
        Map<String, Object> configure = new HashedMap();
        configure.put("a", "b");
        applicationEntity.setConfiguration(configure);
        applicationEntity.setContext(configure);
        ApplicationEntity entity = applicationEntityService.create(applicationEntity);
        Assert.assertNotNull(entity);
        String siteId = entity.getSite().getSiteId();
        Collection resultEntities = applicationEntityService.findBySiteId(siteId);
        Assert.assertEquals(1, resultEntities.size());
    }

    @Test
    public void testGetBySiteIdAndAppType() {
        SiteEntity siteEntity = new SiteEntity();
        siteEntity.setSiteId("testsiteid");
        siteEntity.setSiteName("testsitename");
        siteEntity.setDescription("testdesc");
        ApplicationDesc applicationDesc = new ApplicationDesc();
        applicationDesc.setType("type1");
        ApplicationEntity applicationEntity = new ApplicationEntity();
        applicationEntity.setSite(siteEntity);
        applicationEntity.setDescriptor(applicationDesc);
        applicationEntity.setMode(ApplicationEntity.Mode.LOCAL);
        applicationEntity.setJarPath(applicationDesc.getJarPath());
        Map<String, Object> configure = new HashedMap();
        configure.put("a", "b");
        applicationEntity.setConfiguration(configure);
        applicationEntity.setContext(configure);
        ApplicationEntity entity = applicationEntityService.create(applicationEntity);
        Assert.assertNotNull(entity);
        String siteId = entity.getSite().getSiteId();
        String appType = entity.getDescriptor().getType();
        ApplicationEntity resultEntity = applicationEntityService.getBySiteIdAndAppType(siteId, appType);
        Assert.assertNotNull(resultEntity);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetByUUIDOrAppIdWhenBothNull() {
        applicationEntityService.getByUUIDOrAppId(null, null);
    }

    @Test
    public void testGetByUUIDOrAppIdWhenNullAppId() {
        SiteEntity siteEntity = new SiteEntity();
        siteEntity.setSiteId("testsiteid");
        siteEntity.setSiteName("testsitename");
        siteEntity.setDescription("testdesc");
        ApplicationDesc applicationDesc = new ApplicationDesc();
        applicationDesc.setType("type1");
        ApplicationEntity applicationEntity = new ApplicationEntity();
        applicationEntity.setSite(siteEntity);
        applicationEntity.setDescriptor(applicationDesc);
        applicationEntity.setMode(ApplicationEntity.Mode.LOCAL);
        applicationEntity.setJarPath(applicationDesc.getJarPath());
        Map<String, Object> configure = new HashedMap();
        configure.put("a", "b");
        applicationEntity.setConfiguration(configure);
        applicationEntity.setContext(configure);
        ApplicationEntity entity = applicationEntityService.create(applicationEntity);
        String uuid = entity.getUuid();
        ApplicationEntity resultEntity = applicationEntityService.getByUUIDOrAppId(uuid, null);
        Assert.assertNotNull(resultEntity);
    }

    @Test
    public void testGetByUUIDOrAppIdNullUUID() {
        SiteEntity siteEntity = new SiteEntity();
        siteEntity.setSiteId("testsiteid");
        siteEntity.setSiteName("testsitename");
        siteEntity.setDescription("testdesc");
        ApplicationDesc applicationDesc = new ApplicationDesc();
        applicationDesc.setType("type1");
        ApplicationEntity applicationEntity = new ApplicationEntity();
        applicationEntity.setSite(siteEntity);
        applicationEntity.setDescriptor(applicationDesc);
        applicationEntity.setMode(ApplicationEntity.Mode.LOCAL);
        applicationEntity.setJarPath(applicationDesc.getJarPath());
        Map<String, Object> configure = new HashedMap();
        configure.put("a", "b");
        applicationEntity.setConfiguration(configure);
        applicationEntity.setContext(configure);
        ApplicationEntity entity = applicationEntityService.create(applicationEntity);
        String appId = entity.getAppId();
        ApplicationEntity resultEntity = applicationEntityService.getByUUIDOrAppId(null, appId);
        Assert.assertNotNull(resultEntity);
    }

    @Test
    public void testDelete() {
        SiteEntity siteEntity = new SiteEntity();
        siteEntity.setSiteId("testsiteid");
        siteEntity.setSiteName("testsitename");
        siteEntity.setDescription("testdesc");
        ApplicationDesc applicationDesc = new ApplicationDesc();
        applicationDesc.setType("type1");
        ApplicationEntity applicationEntity = new ApplicationEntity();
        applicationEntity.setSite(siteEntity);
        applicationEntity.setDescriptor(applicationDesc);
        applicationEntity.setMode(ApplicationEntity.Mode.LOCAL);
        applicationEntity.setJarPath(applicationDesc.getJarPath());
        Map<String, Object> configure = new HashedMap();
        configure.put("a", "b");
        applicationEntity.setConfiguration(configure);
        applicationEntity.setContext(configure);
        ApplicationEntity entity = applicationEntityService.create(applicationEntity);
        ApplicationEntity resultEntity = applicationEntityService.delete(entity);
        Assert.assertEquals(resultEntity, entity);
    }

    @Test
    public void testUpdate() {
        SiteEntity siteEntity = new SiteEntity();
        siteEntity.setSiteId("testsiteid");
        siteEntity.setSiteName("testsitename");
        siteEntity.setDescription("testdesc");
        ApplicationDesc applicationDesc = new ApplicationDesc();
        applicationDesc.setType("type1");
        ApplicationEntity applicationEntity = new ApplicationEntity();
        applicationEntity.setSite(siteEntity);
        applicationEntity.setDescriptor(applicationDesc);
        applicationEntity.setMode(ApplicationEntity.Mode.LOCAL);
        applicationEntity.setJarPath(applicationDesc.getJarPath());
        Map<String, Object> configure = new HashedMap();
        configure.put("a", "b");
        applicationEntity.setConfiguration(configure);
        applicationEntity.setContext(configure);
        ApplicationEntity entity = applicationEntityService.create(applicationEntity);
        ApplicationEntity entity2 = entity;
        configure.put("c", "d");
        entity2.setContext(configure);
        ApplicationEntity resultEntity = applicationEntityService.update(entity2);
        org.junit.Assert.assertEquals(2, resultEntity.getContext().size());
        Collection collection = applicationEntityService.findAll();
        Assert.assertEquals(1, collection.size());
    }
}
