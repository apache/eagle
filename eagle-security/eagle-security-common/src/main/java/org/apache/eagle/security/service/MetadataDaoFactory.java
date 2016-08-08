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

import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;

/**
 * @since Apr 12, 2016
 *
 */
public class MetadataDaoFactory {
    private static final Logger LOG = LoggerFactory.getLogger(MetadataDaoFactory.class);

    public static ISecurityMetadataDAO getMetadataDAO(String storeCls) {
        ISecurityMetadataDAO dao = null;
        if (storeCls == null) {
            LOG.warn("metadata store is not configured, use in-memory store !!!");
            dao = new InMemMetadataDaoImpl();
        } else {
            Class<?> clz;
            try {
                clz = Thread.currentThread().getContextClassLoader().loadClass(storeCls);
                if (ISecurityMetadataDAO.class.isAssignableFrom(clz)) {
                    Constructor<?> cotr = clz.getConstructor();
                    dao = (ISecurityMetadataDAO) cotr.newInstance();
                } else {
                    throw new Exception("metadataDao configuration need to be implementation of IMetadataDao! ");
                }
            } catch (Exception e) {
                LOG.error("error when initialize the dao, fall back to in memory mode!", e);
                dao = new InMemMetadataDaoImpl();
            }
        }
        return dao;
    }
}
