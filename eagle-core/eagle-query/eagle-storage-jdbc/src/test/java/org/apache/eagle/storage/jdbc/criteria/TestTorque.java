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
package org.apache.eagle.storage.jdbc.criteria;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.torque.ColumnImpl;
import org.apache.torque.Torque;
import org.apache.torque.TorqueException;
import org.apache.torque.criteria.Criteria;
import org.apache.torque.criteria.SqlEnum;
import org.apache.torque.sql.Query;
import org.apache.torque.sql.SqlBuilder;
import org.apache.torque.util.BasePeerImpl;
import org.apache.torque.util.ColumnValues;
import org.apache.torque.util.JdbcTypedValue;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.UUID;

/**
 * @since 3/27/15
 */
public class TestTorque {
    final static Logger LOG = LoggerFactory.getLogger(TestTorque.class);

    //@Before
    public void setUp() throws TorqueException {
        Configuration configuration = new BaseConfiguration();
        configuration.addProperty("torque.database.default","eagle");
        configuration.addProperty("torque.database.eagle.adapter","mysql");
        configuration.addProperty("torque.dsfactory.eagle.factory","org.apache.torque.dsfactory.SharedPoolDataSourceFactory");

        configuration.addProperty("torque.dsfactory.eagle.connection.driver","org.gjt.mm.mysql.Driver");
        configuration.addProperty("torque.dsfactory.eagle.connection.url","jdbc:mysql://localhost:3306/eagle");
        configuration.addProperty("torque.dsfactory.eagle.connection.user","eagle");
        configuration.addProperty("torque.dsfactory.eagle.connection.password","eagle");

        Torque.init(configuration);
    }

    //@Test
    public void testSelect() throws TorqueException, SQLException {
        Criteria crit = new Criteria();
        crit.setDbName("eagle");
        crit.addSelectColumn(new ColumnImpl("column1"));
        crit.addSelectColumn(new ColumnImpl("column2"));
        crit.addSelectColumn(new ColumnImpl("column2/100"));

        crit.where(new ColumnImpl("column1"),SqlEnum.GREATER_EQUAL);

        crit.addFrom("tableName");
        crit.addAlias("column1", "c1");
        crit.addGroupByColumn(new ColumnImpl("column1"));
        crit.addAscendingOrderByColumn(new ColumnImpl("column3"));
        crit.setLimit(1000);

        Query query = SqlBuilder.buildQuery(crit);

        String sql = query.toString();
        System.out.println(sql);

        Connection connection = Torque.getConnection();
        PreparedStatement statement = connection.prepareStatement(sql);
        statement.setInt(1,1000);

        try {
            ResultSet result = statement.executeQuery();
        }catch (SQLException ex){
            LOG.warn(ex.getMessage(),ex);
        } finally {
            connection.close();
        }
    }

    //@Test
    public void testDelete() throws TorqueException, SQLException {
        Criteria crit = new Criteria();
        crit.where(new ColumnImpl("unittest_testtsentity", "field1"), 2.0);
        System.out.println(crit.toString());

        BasePeerImpl basePeer = new BasePeerImpl();
        basePeer.setDatabaseName("eagle");
        basePeer.doDelete(crit);
    }


    //@Test
    public void testInsert() throws TorqueException {
        BasePeerImpl basePeer = new BasePeerImpl();
        basePeer.setDatabaseName("eagle");
        ColumnValues columnValues = new ColumnValues();
        columnValues.put(new ColumnImpl("unittest_testtsentity","uuid"),new JdbcTypedValue(UUID.randomUUID().toString(),Types.VARCHAR));
        columnValues.put(new ColumnImpl("unittest_testtsentity","field1"),new JdbcTypedValue(34,Types.INTEGER));
        basePeer.doInsert(columnValues);
    }

    @Test
    public void test() {

    }
}
