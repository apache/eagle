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

import org.apache.eagle.metadata.exceptions.EntityNotFoundException;
import org.apache.eagle.metadata.model.SiteEntity;
import org.apache.eagle.metadata.service.SiteEntityService;
import org.junit.Assert;
import org.junit.Test;

import javax.inject.Inject;
import java.sql.SQLException;
import java.util.Collection;

public class SiteEntityServiceJDBCImplTest extends JDBCMetadataTestBase {
    @Inject
    SiteEntityService siteEntityService;

    @Test
    public void testInsertSiteEntityAndFindAll() throws SQLException {
        SiteEntity siteEntity = new SiteEntity();
        siteEntity.setSiteId("testsiteid");
        siteEntity.setSiteName("testsitename");
        siteEntity.setDescription("testdesc");

        siteEntityService.create(siteEntity);
        String uuid = siteEntity.getUuid();
        long createdTime = siteEntity.getCreatedTime();
        long modifiedTime = siteEntity.getModifiedTime();

        Collection<SiteEntity> results = siteEntityService.findAll();
        Assert.assertEquals(1, results.size());
        SiteEntity siteEntityFromDB = results.iterator().next();
        Assert.assertEquals(uuid, siteEntityFromDB.getUuid());
        Assert.assertEquals("testsiteid", siteEntityFromDB.getSiteId());
        Assert.assertEquals("testsitename", siteEntityFromDB.getSiteName());
        Assert.assertEquals("testdesc", siteEntityFromDB.getDescription());
        Assert.assertEquals(createdTime, siteEntityFromDB.getCreatedTime());
        Assert.assertEquals(modifiedTime, siteEntityFromDB.getModifiedTime());

    }

    @Test(expected = IllegalArgumentException.class)
    public void testInsertSiteEntitySiteIdUnique() throws SQLException {
        SiteEntity siteEntity = new SiteEntity();
        siteEntity.setSiteId("testsiteid");
        siteEntity.setSiteName("testsitename");
        siteEntity.setDescription("testdesc");
        siteEntityService.create(siteEntity);


        SiteEntity siteEntity1 = new SiteEntity();
        siteEntity1.setSiteId("testsiteid");
        siteEntity1.setSiteName("testsitename");
        siteEntity1.setDescription("testdesc");
        siteEntityService.create(siteEntity1);


        Collection<SiteEntity> results = siteEntityService.findAll();
        Assert.assertEquals(1, results.size());
    }

    @Test
    public void testInsertSiteEntityAndFindByUUID() throws SQLException, EntityNotFoundException {
        SiteEntity siteEntity = new SiteEntity();
        siteEntity.setSiteId("testsiteid");
        siteEntity.setSiteName("testsitename");
        siteEntity.setDescription("testdesc");
        siteEntityService.create(siteEntity);
        String uuid = siteEntity.getUuid();
        long createdTime = siteEntity.getCreatedTime();
        long modifiedTime = siteEntity.getModifiedTime();

        SiteEntity siteEntityFromDB = siteEntityService.getByUUID(uuid);
        Assert.assertEquals(uuid, siteEntityFromDB.getUuid());
        Assert.assertEquals("testsiteid", siteEntityFromDB.getSiteId());
        Assert.assertEquals("testsitename", siteEntityFromDB.getSiteName());
        Assert.assertEquals("testdesc", siteEntityFromDB.getDescription());
        Assert.assertEquals(createdTime, siteEntityFromDB.getCreatedTime());
        Assert.assertEquals(modifiedTime, siteEntityFromDB.getModifiedTime());

    }

    @Test(expected = EntityNotFoundException.class)
    public void testFindByUUID() throws SQLException, EntityNotFoundException {
        siteEntityService.getByUUID("fake uuid");
    }

    @Test
    public void testInsertSiteEntityAndFindBySiteId() throws SQLException, EntityNotFoundException {
        SiteEntity siteEntity = new SiteEntity();
        siteEntity.setSiteId("testsiteid");
        siteEntity.setSiteName("testsitename");
        siteEntity.setDescription("testdesc");
        siteEntityService.create(siteEntity);
        String uuid = siteEntity.getUuid();
        long createdTime = siteEntity.getCreatedTime();
        long modifiedTime = siteEntity.getModifiedTime();

        SiteEntity siteEntityFromDB = siteEntityService.getBySiteId("testsiteid");
        Assert.assertEquals(uuid, siteEntityFromDB.getUuid());
        Assert.assertEquals("testsiteid", siteEntityFromDB.getSiteId());
        Assert.assertEquals("testsitename", siteEntityFromDB.getSiteName());
        Assert.assertEquals("testdesc", siteEntityFromDB.getDescription());
        Assert.assertEquals(createdTime, siteEntityFromDB.getCreatedTime());
        Assert.assertEquals(modifiedTime, siteEntityFromDB.getModifiedTime());

    }

    @Test(expected = EntityNotFoundException.class)
    public void testFindBySiteId() throws SQLException, EntityNotFoundException {
        siteEntityService.getBySiteId("fake siteId");
    }

    @Test
    public void testUpdateSiteEntity() throws SQLException, EntityNotFoundException {

        SiteEntity siteEntity = new SiteEntity();
        siteEntity.setSiteId("testsiteid");
        siteEntity.setSiteName("testsitename");
        siteEntity.setDescription("testdesc");
        siteEntityService.create(siteEntity);
        String uuid = siteEntity.getUuid();
        long createdTime = siteEntity.getCreatedTime();
        long modifiedTime = siteEntity.getModifiedTime();

        siteEntity.setSiteName("testsitenamem");
        siteEntity.setDescription("testdescm");
        siteEntityService.update(siteEntity);

        SiteEntity siteEntityFromDB = siteEntityService.getByUUID(uuid);
        Assert.assertEquals(uuid, siteEntityFromDB.getUuid());
        Assert.assertEquals("testsiteid", siteEntityFromDB.getSiteId());
        Assert.assertEquals("testsitenamem", siteEntityFromDB.getSiteName());
        Assert.assertEquals("testdescm", siteEntityFromDB.getDescription());
        Assert.assertEquals(createdTime, siteEntityFromDB.getCreatedTime());
        Assert.assertTrue(siteEntityFromDB.getModifiedTime() > modifiedTime);

    }

    @Test(expected = EntityNotFoundException.class)
    public void testUpdateSiteEntityFail() throws SQLException, EntityNotFoundException {
        SiteEntity siteEntity = new SiteEntity();
        siteEntity.setSiteId("testsiteid");
        siteEntity.setSiteName("testsitename");
        siteEntity.setDescription("testdesc");
        siteEntityService.update(siteEntity);
    }

    @Test(expected = EntityNotFoundException.class)
    public void testDeleteSiteEntityByUUID() throws SQLException, EntityNotFoundException {

        SiteEntity siteEntity = new SiteEntity();
        siteEntity.setSiteId("testsiteid");
        siteEntity.setSiteName("testsitename");
        siteEntity.setDescription("testdesc");
        siteEntityService.create(siteEntity);
        String uuid = siteEntity.getUuid();
        Collection<SiteEntity> results = siteEntityService.findAll();
        Assert.assertEquals(1, results.size());
        siteEntityService.deleteByUUID(uuid);
        results = siteEntityService.findAll();
        Assert.assertEquals(0, results.size());
        siteEntityService.getByUUID(uuid);
    }

    @Test(expected = EntityNotFoundException.class)
    public void testDeleteSiteEntityByUUID1() throws SQLException, EntityNotFoundException {
        siteEntityService.getByUUID("fake uuid");
    }

    @Test(expected = EntityNotFoundException.class)
    public void testDeleteSiteEntityBySiteId() throws SQLException, EntityNotFoundException {

        SiteEntity siteEntity = new SiteEntity();
        siteEntity.setSiteId("testsiteid");
        siteEntity.setSiteName("testsitename");
        siteEntity.setDescription("testdesc");
        String siteId = siteEntity.getSiteId();
        siteEntityService.create(siteEntity);
        siteEntityService.findAll();
        Collection<SiteEntity> results = siteEntityService.findAll();
        Assert.assertEquals(1, results.size());
        siteEntityService.deleteBySiteId(siteId);
        siteEntityService.getBySiteId(siteId);
    }

    @Test(expected = EntityNotFoundException.class)
    public void testDeleteSiteEntityBySiteId1() throws SQLException, EntityNotFoundException {
        siteEntityService.getBySiteId("fake siteId");
    }

    @Test
    public void testSiteEntity() throws SQLException, EntityNotFoundException {
        SiteEntity siteEntity1 = new SiteEntity("uuid", "siteid");
        Assert.assertEquals("uuid", siteEntity1.getUuid());
        Assert.assertEquals("siteid", siteEntity1.getSiteId());

    }

}
