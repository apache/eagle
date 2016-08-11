/*
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
package org.apache.eagle.security.crawler.audit;

import com.google.inject.Inject;
import org.apache.eagle.common.module.CommonGuiceModule;
import org.apache.eagle.common.module.GuiceJUnitRunner;
import org.apache.eagle.common.module.Modules;
import org.apache.eagle.metadata.store.jdbc.JDBCMetadataQueryService;
import org.apache.eagle.metadata.store.jdbc.JDBCMetadataStore;
import org.apache.eagle.security.service.HBaseSensitivityEntity;
import org.apache.eagle.security.service.JDBCSecurityMetadataDAO;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;

@RunWith(GuiceJUnitRunner.class)
@Modules({JDBCMetadataStore.class, CommonGuiceModule.class})
public class JDBCSecurityMetadataDAOTest {
    @Inject
    private JDBCSecurityMetadataDAO metadataDAO;

    @Inject
    private JDBCMetadataQueryService queryService;

    @Test
    public void testJDBCSecurityMetadataDAO(){
        HBaseSensitivityEntity entity = new HBaseSensitivityEntity();
        entity.setSite("test_site");
        entity.setHbaseResource("test_hbaseResource");
        metadataDAO.addHBaseSensitivity(Collections.singletonList(entity));
        Collection<HBaseSensitivityEntity> entities = metadataDAO.listHBaseSensitivies();
        Assert.assertEquals(1,entities.size());
        Assert.assertEquals("test_site",entities.iterator().next().getSite());
    }

    @After
    public void after() throws SQLException {
        queryService.dropTable("hbase_sensitivity_entity");
    }
}