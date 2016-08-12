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
package org.apache.eagle.security.service;

import com.google.inject.Inject;
import com.typesafe.config.Config;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * In memory service for simple service start. Make all service API as
 * synchronized.
 *
 * @since Apr 11, 2016
 *
 */
public class InMemMetadataDaoImpl implements ISecurityMetadataDAO {

    private static final Logger LOG = LoggerFactory.getLogger(InMemMetadataDaoImpl.class);

    private Map<Pair<String, String>, HBaseSensitivityEntity> hBaseSensitivityEntities = new HashMap<>();
    private Map<Pair<String, String>, HdfsSensitivityEntity> hdfsSensitivityEntities = new HashMap<>();
    private Map<String, IPZoneEntity> ipZones = new HashMap<>();


    @Inject
    public InMemMetadataDaoImpl() {
    }

    @Override
    public synchronized Collection<HBaseSensitivityEntity> listHBaseSensitivies() {
        return hBaseSensitivityEntities.values();
    }

    @Override
    public synchronized OpResult addHBaseSensitivity(Collection<HBaseSensitivityEntity> h) {
        for (HBaseSensitivityEntity e : h) {
            Pair p = new ImmutablePair<>(e.getSite(), e.getHbaseResource());
            hBaseSensitivityEntities.put(p, e);
        }
        return new OpResult();
    }

    @Override
    public Collection<HdfsSensitivityEntity> listHdfsSensitivities() {
        return hdfsSensitivityEntities.values();
    }

    @Override
    public OpResult addHdfsSensitivity(Collection<HdfsSensitivityEntity> h) {
        for(HdfsSensitivityEntity e : h){
            Pair p = new ImmutablePair<>(e.getSite(), e.getFiledir());
            hdfsSensitivityEntities.put(p, e);
        }
        return new OpResult();
    }

    @Override
    public Collection<IPZoneEntity> listIPZones() {
        return ipZones.values();
    }

    @Override
    public OpResult addIPZone(Collection<IPZoneEntity> h) {
        for(IPZoneEntity e : h){
            ipZones.put(e.getIphost(), e);
        }
        return new OpResult();
    }
}
