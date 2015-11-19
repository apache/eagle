package org.apache.eagle.service.security.hive;/*
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
import org.apache.eagle.service.security.hive.dao.HiveMetadataAccessConfig;
import org.apache.eagle.service.security.hive.dao.HiveMetadataByHiveServer2DAOImpl;
import org.apache.eagle.service.security.hive.dao.HiveMetadataByMetastoreDBDAOImpl;
import org.apache.eagle.service.security.hive.dao.HiveMetadataDAO;

import org.junit.Test;

public class TestHiveMetadataDAO {
   // @Test
    public void testHiveMetadataByMetastoreDB() throws Exception{
        HiveMetadataAccessConfig config = new HiveMetadataAccessConfig();
        config.setJdbcDriverClassName("com.mysql.jdbc.Driver");
        config.setJdbcUrl("jdbc:mysql://sandbox.hortonworks.com:3306/hive");
        config.setUser("hive");
        config.setPassword("hive");
        HiveMetadataByMetastoreDBDAOImpl impl = new HiveMetadataByMetastoreDBDAOImpl(config);
        System.out.println(impl.getDatabases());
        System.out.println(impl.getTables("xademo"));
        System.out.println(impl.getColumns("xademo", "call_detail_records"));
    }

    //@Test
    public void testHiveMetadataByHiveServer2() throws Exception{
        HiveMetadataAccessConfig config = new HiveMetadataAccessConfig();
        config.setJdbcDriverClassName("org.apache.hive.jdbc.HiveDriver");
        config.setJdbcUrl("jdbc:hive2://sandbox.hortonworks.com:10000/xademo");
        config.setUser("hive");
        config.setPassword("hive");
        HiveMetadataDAO impl = new HiveMetadataByHiveServer2DAOImpl(config);
        System.out.println(impl.getDatabases());
        System.out.println(impl.getTables("xademo"));
        System.out.println(impl.getColumns("xademo", "call_detail_records"));
    }

    @Test
    public void test() {

    }
}