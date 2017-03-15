/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.eagle.metadata.store.jdbc;


import com.google.inject.Inject;
import org.apache.eagle.metadata.exceptions.EntityNotFoundException;
import org.apache.eagle.metadata.model.DashboardEntity;
import org.apache.eagle.metadata.service.DashboardEntityService;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;

public class DashboardEntityServiceJDBCImplTest extends JDBCMetadataTestBase {
    @Inject
    private DashboardEntityService dashboardEntityService;

    @Test
    public void testDashboardCRUD() throws EntityNotFoundException {
        DashboardEntity entity = new DashboardEntity();
        {
            entity.setName("Sample Dashboard");
            entity.setDescription("A sample dashboard for unit test");
            entity.setAuthor("somebody");
            entity.setSettings(new HashMap<String, Object>() {
                {
                    put("Stringkey", "SettingValue");
                }
            });
            entity.setCharts(Arrays.asList(
                "{chartType: 'LINE'}",
                "{chartType}: 'PIE'"
            ));
        }

        {
            Assert.assertEquals(0, dashboardEntityService.findAll().size());
        }

        DashboardEntity createdEntity;
        {
            createdEntity = dashboardEntityService.create(entity);
            Assert.assertNotNull(createdEntity.getUuid());
            Assert.assertTrue(createdEntity.getCreatedTime() > 0);
            Assert.assertTrue(createdEntity.getModifiedTime() > 0);
            Assert.assertEquals(1, dashboardEntityService.findAll().size());
        }

        DashboardEntity updatedEntity;
        {
            DashboardEntity entityToUpdate = new DashboardEntity();
            entityToUpdate.setUuid(createdEntity.getUuid());
            entityToUpdate.setName("Sample Dashboard (Updated)");
            updatedEntity = dashboardEntityService.update(entityToUpdate);
            Assert.assertEquals(createdEntity.getUuid(), updatedEntity.getUuid());
            Assert.assertEquals("Sample Dashboard (Updated)", updatedEntity.getName());
            Assert.assertEquals(createdEntity.getCreatedTime(), updatedEntity.getCreatedTime());
            Assert.assertTrue(updatedEntity.getModifiedTime() > createdEntity.getModifiedTime());
            Assert.assertEquals(createdEntity.getAuthor(),updatedEntity.getAuthor());
            Assert.assertEquals(createdEntity.getCharts(), updatedEntity.getCharts());
            Assert.assertEquals(createdEntity.getSettings(), updatedEntity.getSettings());
            Assert.assertEquals(1, dashboardEntityService.findAll().size());
        }

        DashboardEntity deletedEntity;
        {
            deletedEntity = dashboardEntityService.deleteByUUID(updatedEntity.getUuid());
            Assert.assertEquals(updatedEntity, deletedEntity);
            Assert.assertEquals(0, dashboardEntityService.findAll().size());
        }
    }
}
