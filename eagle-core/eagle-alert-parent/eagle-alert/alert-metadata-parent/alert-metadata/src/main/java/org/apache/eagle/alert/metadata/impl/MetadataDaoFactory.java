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
package org.apache.eagle.alert.metadata.impl;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.eagle.alert.metadata.IMetadataDao;
import org.apache.eagle.alert.metadata.MetadataUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;

/**
 * @since Apr 12, 2016.
 */
public class MetadataDaoFactory {

    private static final Logger LOG = LoggerFactory.getLogger(MetadataDaoFactory.class);

    private static final MetadataDaoFactory INSTANCE = new MetadataDaoFactory();

    private IMetadataDao dao;

    private MetadataDaoFactory() {
        Config config = ConfigFactory.load();
        if (!config.hasPath(MetadataUtils.META_DATA)) {
            LOG.warn("metadata is not configured, use in-memory store !!!");
            dao = new InMemMetadataDaoImpl(null);
        } else {
            Config metaDataConfig = config.getConfig(MetadataUtils.META_DATA);
            try {
                String clsName = metaDataConfig.getString(MetadataUtils.ALERT_META_DATA_DAO);
                Class<?> clz;
                clz = Thread.currentThread().getContextClassLoader().loadClass(clsName);
                if (IMetadataDao.class.isAssignableFrom(clz)) {
                    Constructor<?> cotr = clz.getConstructor(Config.class);
                    LOG.info("metadata.alertMetadataDao loaded: " + clsName);
                    dao = (IMetadataDao) cotr.newInstance(metaDataConfig);
                } else {
                    throw new Exception("metadata.metadataDao configuration need to be implementation of IMetadataDao! ");
                }
            } catch (Exception e) {
                LOG.error("error when initialize the dao, fall back to in memory mode!", e);
                dao = new InMemMetadataDaoImpl(metaDataConfig);
            }
        }
    }

    public static MetadataDaoFactory getInstance() {
        return INSTANCE;
    }

    public IMetadataDao getMetadataDao() {
        return dao;
    }
}
