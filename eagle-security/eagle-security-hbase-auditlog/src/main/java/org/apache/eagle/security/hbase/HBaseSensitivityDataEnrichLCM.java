/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.eagle.security.hbase;

import com.typesafe.config.Config;
import org.apache.eagle.security.service.HBaseSensitivityEntity;
import org.apache.eagle.security.service.ISecurityDataEnrichServiceClient;
import org.apache.eagle.security.enrich.AbstractDataEnrichLCM;

import java.util.Collection;

/**
 * Since 8/16/16.
 */
public class HBaseSensitivityDataEnrichLCM extends AbstractDataEnrichLCM<HBaseSensitivityEntity, String> {
    public HBaseSensitivityDataEnrichLCM(Config config){
        super(config);
    }
    @Override
    protected Collection<HBaseSensitivityEntity> loadFromService(ISecurityDataEnrichServiceClient client) {
        return client.listHBaseSensitivities();
    }

    @Override
    public String getCacheKey(HBaseSensitivityEntity entity) {
        return entity.getHBaseResource();
    }
}
