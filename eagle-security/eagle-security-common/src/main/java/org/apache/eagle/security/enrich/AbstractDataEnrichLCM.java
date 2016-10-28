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

package org.apache.eagle.security.enrich;

import com.typesafe.config.Config;
import org.apache.eagle.security.service.ISecurityDataEnrichServiceClient;
import org.apache.eagle.security.service.SecurityDataEnrichServiceClientImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

/**
 * Since 8/16/16.
 */
public abstract class AbstractDataEnrichLCM<T, K> implements DataEnrichLCM<T, K> {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractDataEnrichLCM.class);
    private Config config;
    public AbstractDataEnrichLCM(Config config){
        this.config = config;
    }

    @Override
    public Collection<T> loadExternal() {
        String eagleServiceHost = config.getString("service.host");
        Integer eagleServicePort = config.getInt("service.port");

        // load from eagle database
        LOG.info("Load sensitivity information from eagle service " + eagleServiceHost + ":" + eagleServicePort);

        ISecurityDataEnrichServiceClient client = new SecurityDataEnrichServiceClientImpl(eagleServiceHost, eagleServicePort, "/rest");
        return loadFromService(client);
    }

    protected abstract Collection<T> loadFromService(ISecurityDataEnrichServiceClient client);
}
