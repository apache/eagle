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
package org.apache.eagle.service.client;

import org.apache.eagle.common.config.EagleConfigFactory;
import org.apache.eagle.log.entity.GenericMetricEntity;
import org.apache.eagle.log.entity.GenericServiceAPIResponseEntity;
import org.apache.eagle.log.entity.meta.EntityDefinition;
import org.apache.eagle.log.entity.meta.EntityDefinitionManager;
import org.apache.eagle.service.client.impl.EagleServiceClientImpl;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TestIEagleServiceClient extends ClientTestBase {
    IEagleServiceClient client;
    //@Before
    public void setUp(){
        client = new EagleServiceClientImpl("localhost", EagleConfigFactory.load().getServicePort());
    }

    /**
     * Just compiling passed is ok
     */
    //@Test
    @SuppressWarnings("unused")
    public void testCreate() throws IOException, EagleServiceClientException, IllegalAccessException, InstantiationException {
        EntityDefinition entityDefinition = EntityDefinitionManager.getEntityDefinitionByEntityClass(GenericMetricEntity.class);
        hbase.createTable(entityDefinition.getTable(), entityDefinition.getColumnFamily());

        client = new EagleServiceClientImpl("localhost", EagleConfigFactory.load().getServicePort());
        List<GenericMetricEntity> metricEntityList = new ArrayList<GenericMetricEntity>();
        GenericServiceAPIResponseEntity<String> unTypedResponse = client.create(metricEntityList);
        GenericServiceAPIResponseEntity<String> weakTypedResponse = client.create(metricEntityList,GenericMetricEntity.GENERIC_METRIC_SERVICE);
        GenericServiceAPIResponseEntity<String> strongTypedResponse = client.create(metricEntityList,GenericMetricEntity.class);

        GenericServiceAPIResponseEntity<GenericMetricEntity> weakTypedSearchResponse = client.search("").send();
        if(weakTypedSearchResponse!=null) {
            Class<GenericMetricEntity> typedClazz = weakTypedSearchResponse.getType();
            List<GenericMetricEntity> typedEntities = weakTypedSearchResponse.getObj();
        }
    }

    @Test
    public void test() {

    }
}