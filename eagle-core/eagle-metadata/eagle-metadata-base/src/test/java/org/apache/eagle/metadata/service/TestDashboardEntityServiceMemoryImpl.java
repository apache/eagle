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
package org.apache.eagle.metadata.service;

import org.apache.eagle.common.security.User;
import org.apache.eagle.metadata.exceptions.EntityNotFoundException;
import org.apache.eagle.metadata.model.DashboardEntity;
import org.apache.eagle.metadata.service.memory.DashboardEntityServiceMemoryImpl;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;

public class TestDashboardEntityServiceMemoryImpl  {
    private DashboardEntityService dashboardEntityService = new DashboardEntityServiceMemoryImpl();
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void testDashboardCRUD() throws EntityNotFoundException {
        User user1 = new User();
        user1.setName("user1");
        user1.setRoles(Collections.singleton(User.Role.USER));

        User user2 = new User();
        user2.setName("user2");
        user2.setRoles(Collections.singleton(User.Role.USER));

        User admin  = new User();
        admin.setName("admin");
        admin.setRoles(Collections.singleton(User.Role.ADMINISTRATOR));

        DashboardEntity entity = new DashboardEntity();
        {
            entity.setName("Sample Dashboard");
            entity.setDescription("A sample dashboard for unit test");
            entity.setAuthor(user1.getName());
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
            updatedEntity = dashboardEntityService.update(entityToUpdate, user1);
            Assert.assertEquals(createdEntity.getUuid(), updatedEntity.getUuid());
            Assert.assertEquals("Sample Dashboard (Updated)", updatedEntity.getName());
            Assert.assertEquals(createdEntity.getCreatedTime(), updatedEntity.getCreatedTime());
            Assert.assertEquals(createdEntity.getAuthor(),updatedEntity.getAuthor());
            Assert.assertEquals(createdEntity.getCharts(), updatedEntity.getCharts());
            Assert.assertEquals(createdEntity.getSettings(), updatedEntity.getSettings());
            Assert.assertEquals(1, dashboardEntityService.findAll().size());
        }

        DashboardEntity deletedEntity;

        {
            expectedException.expect(IllegalArgumentException.class);
            dashboardEntityService.deleteByUUID(updatedEntity.getUuid(), user2);
            Assert.assertEquals(1, dashboardEntityService.findAll().size());
        }

        {
            deletedEntity = dashboardEntityService.deleteByUUID(updatedEntity.getUuid(), admin);
            Assert.assertEquals(updatedEntity, deletedEntity);
            Assert.assertEquals(0, dashboardEntityService.findAll().size());
        }
    }
}
