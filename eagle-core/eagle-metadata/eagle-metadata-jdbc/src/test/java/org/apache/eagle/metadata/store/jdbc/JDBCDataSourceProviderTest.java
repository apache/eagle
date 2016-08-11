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

import com.google.inject.Inject;
import org.junit.Assert;
import org.junit.Test;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class JDBCDataSourceProviderTest extends JDBCMetadataTestBase{
    @Inject
    private DataSource dataSource;
    @Inject
    private JDBCDataSourceConfig dataSourceConfig;

    @Test
    public void testSingletonDataSource(){
        DataSource dataSource1 = injector().getInstance(DataSource.class);
        DataSource dataSource2 = injector().getInstance(DataSource.class);
        Assert.assertTrue("Should get datasource in singleton pattern",dataSource == dataSource1);
        Assert.assertTrue("Should get datasource in singleton pattern",dataSource1 == dataSource2);
    }

    @Test
    public void testDataSourceConfig(){
        Assert.assertEquals("jdbc:h2:./eagle4ut",dataSourceConfig.getUrl());
        Assert.assertEquals(null,dataSourceConfig.getUsername());
        Assert.assertEquals(null,dataSourceConfig.getPassword());
        Assert.assertEquals("encoding=UTF8;timeout=60",dataSourceConfig.getConnectionProperties());
    }

    @Test
    public void testConnection() throws SQLException {
        Connection connection = null;
        Statement statement = null;
        ResultSet resultSet = null;
        try {
            connection = dataSource.getConnection();
            Assert.assertNotNull(connection);
            statement = connection.createStatement();
            resultSet  = statement.executeQuery("SELECT 1");
            Assert.assertTrue(resultSet.next());
            Assert.assertEquals(1,resultSet.getInt(1));
        } catch (SQLException e) {
            Assert.fail(e.getMessage());
            throw e;
        } finally {
            if(resultSet!=null)
                resultSet.close();
            if(statement != null)
                statement.close();
            if(connection!=null)
                connection.close();
        }
    }
}