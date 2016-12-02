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
import org.apache.eagle.metadata.exceptions.EntityNotFoundException;
import org.apache.eagle.metadata.model.SiteEntity;
import org.apache.eagle.metadata.service.memory.SiteEntityEntityServiceMemoryImpl;
import org.junit.Test;

/**
 * @Since 11/23/16.
 */
public class TestSiteEntityServiceMemoryImpl {

    SiteEntityService siteEntityService = new SiteEntityEntityServiceMemoryImpl();

    @Test
    public void testCreate() {
        SiteEntity siteEntity = new SiteEntity();
        siteEntity.setSiteId("testsiteid");
        siteEntity.setSiteName("testsitename");
        siteEntity.setDescription("testdesc");
        SiteEntity resultEntity = siteEntityService.create(siteEntity);
        Assert.assertNotNull(resultEntity);
    }

    @Test
    public void testGetBySiteIdAndUUID() throws EntityNotFoundException {
        SiteEntity siteEntity = new SiteEntity();
        siteEntity.setSiteId("testsiteid");
        siteEntity.setSiteName("testsitename");
        siteEntity.setDescription("testdesc");
        SiteEntity entity = siteEntityService.create(siteEntity);
        String uuid = entity.getUuid();
        SiteEntity resultEntity = siteEntityService.getByUUID(uuid);
        Assert.assertNotNull(resultEntity);
        String siteId = entity.getSiteId();
        resultEntity = siteEntityService.getBySiteId(siteId);
        Assert.assertNotNull(resultEntity);

    }

    @Test(expected = EntityNotFoundException.class)
    public void testGetBySiteIdFail() throws EntityNotFoundException {
        siteEntityService.getBySiteId("fake uuid");
    }

    @Test
    public void testDeleteBySiteId() throws EntityNotFoundException {
        SiteEntity siteEntity = new SiteEntity();
        siteEntity.setSiteId("testsiteid");
        siteEntity.setSiteName("testsitename");
        siteEntity.setDescription("testdesc");
        SiteEntity entity = siteEntityService.create(siteEntity);
        SiteEntity resultEntity = siteEntityService.deleteBySiteId(entity.getSiteId());
        Assert.assertEquals(resultEntity, entity);
    }

    @Test
    public void testDeleteBtUUID() throws EntityNotFoundException {
        SiteEntity siteEntity = new SiteEntity();
        siteEntity.setSiteId("testsiteid");
        siteEntity.setSiteName("testsitename");
        siteEntity.setDescription("testdesc");
        SiteEntity entity = siteEntityService.create(siteEntity);
        SiteEntity resultEntity = siteEntityService.deleteByUUID(entity.getUuid());
        Assert.assertEquals(resultEntity, entity);
    }

    @Test
    public void testUpdate() throws EntityNotFoundException {
        SiteEntity siteEntity = new SiteEntity();
        siteEntity.setSiteId("testsiteid");
        siteEntity.setSiteName("testsitename");
        siteEntity.setDescription("testdesc");
        SiteEntity entity = siteEntityService.create(siteEntity);
        SiteEntity entity2 = entity;
        entity2.setSiteName("testsitename2");
        SiteEntity resultEntity = siteEntityService.update(entity2);
        Assert.assertEquals("testsitename2", resultEntity.getSiteName());
    }
}
