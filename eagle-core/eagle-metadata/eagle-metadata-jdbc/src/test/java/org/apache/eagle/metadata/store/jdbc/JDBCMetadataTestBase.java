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
package org.apache.eagle.metadata.store.jdbc;

import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import org.apache.commons.dbcp.BasicDataSource;
import org.apache.eagle.app.module.ApplicationGuiceModule;
import org.apache.eagle.common.module.CommonGuiceModule;
import org.junit.After;
import org.junit.Before;

import javax.sql.DataSource;
import java.sql.SQLException;

public class JDBCMetadataTestBase {
    private Injector injector;

    @Inject
    private DataSource dataSource;

    @Before
    public void setUp() {
        injector = Guice.createInjector(new JDBCMetadataStore(), new ApplicationGuiceModule(), new CommonGuiceModule());
        injector.injectMembers(this);
    }

    @After
    public void after() throws SQLException {
        if (dataSource != null) {
            ((BasicDataSource) dataSource).close();
        }
    }

    public Injector injector() {
        return injector;
    }
}
