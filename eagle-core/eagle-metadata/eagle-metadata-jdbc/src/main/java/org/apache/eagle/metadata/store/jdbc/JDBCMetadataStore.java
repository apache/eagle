/**
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
package org.apache.eagle.metadata.store.jdbc;


import com.google.inject.Singleton;
import org.apache.eagle.alert.metadata.IMetadataDao;
import org.apache.eagle.alert.metadata.MetadataUtils;
import org.apache.eagle.alert.metadata.impl.JdbcMetadataDaoImpl;
import org.apache.eagle.metadata.persistence.MetadataStore;
import org.apache.eagle.metadata.service.ApplicationEntityService;
import org.apache.eagle.metadata.service.DashboardEntityService;
import org.apache.eagle.metadata.service.SiteEntityService;
import org.apache.eagle.metadata.store.jdbc.provider.JDBCDataSourceProvider;
import org.apache.eagle.metadata.store.jdbc.provider.JDBCMetadataStoreConfigProvider;
import org.apache.eagle.metadata.store.jdbc.service.ApplicationEntityServiceJDBCImpl;
import org.apache.eagle.metadata.store.jdbc.service.DashboardEntityServiceJDBCImpl;
import org.apache.eagle.metadata.store.jdbc.service.SiteEntityServiceJDBCImpl;

import javax.sql.DataSource;

public class JDBCMetadataStore extends MetadataStore {
    @Override
    protected void configure() {
        bind(IMetadataDao.class).to(JdbcMetadataDaoImpl.class).in(Singleton.class);
        bind(DataSource.class).toProvider(JDBCDataSourceProvider.class).in(Singleton.class);
        bind(JDBCDataSourceConfig.class).toProvider(JDBCMetadataStoreConfigProvider.class).in(Singleton.class);
        bind(JDBCMetadataQueryService.class).to(JDBCMetadataMetadataStoreServiceImpl.class).in(Singleton.class);
        bind(ApplicationEntityService.class).to(ApplicationEntityServiceJDBCImpl.class).in(Singleton.class);
        bind(SiteEntityService.class).to(SiteEntityServiceJDBCImpl.class).in(Singleton.class);
        bind(DashboardEntityService.class).to(DashboardEntityServiceJDBCImpl.class).in(Singleton.class);
    }
}