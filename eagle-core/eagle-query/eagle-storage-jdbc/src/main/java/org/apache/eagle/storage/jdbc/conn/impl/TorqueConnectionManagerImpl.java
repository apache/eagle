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
package org.apache.eagle.storage.jdbc.conn.impl;

import org.apache.eagle.storage.jdbc.JdbcConstants;
import org.apache.eagle.storage.jdbc.conn.ConnectionConfig;
import org.apache.eagle.storage.jdbc.conn.ConnectionManager;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.torque.Torque;
import org.apache.torque.TorqueException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;

/**
 * @since 3/27/15
 */
public class TorqueConnectionManagerImpl implements ConnectionManager {
    private final static Logger LOG = LoggerFactory.getLogger(TorqueConnectionManagerImpl.class);
    public final static String DEFAULT_DATA_SOURCE_FACTORY_CLASS = "org.apache.torque.dsfactory.SharedPoolDataSourceFactory";

    private ConnectionConfig config;

    @Override
    public void init(ConnectionConfig config) throws Exception {
        if(this.config != null) LOG.warn("Resetting config");
        this.config = config;

        if(!Torque.isInit()) {
            try {
                if(LOG.isDebugEnabled()) LOG.debug("Apache Torque initializing");
                Torque.init(buildConfiguration(config));
            } catch (TorqueException e) {
                throw new Exception(e);
            }
        }else{
            LOG.warn("Apache Torque has already been initialized, ignore");
        }
    }

    @Override
    public ConnectionConfig getConfig() {
        return this.config;
    }

    @Override
    public Connection getConnection() throws Exception {
        try {
            return Torque.getConnection();
        } catch (TorqueException e) {
            LOG.error("Failed to get connection",e);
            throw new Exception(e);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public TorqueStatementPeerImpl getStatementExecutor() {
        TorqueStatementPeerImpl statementPeer = new TorqueStatementPeerImpl();
        statementPeer.init(this.getConfig());
        return statementPeer;
    }

    @SuppressWarnings("unchecked")
    @Override
    public TorqueStatementPeerImpl getStatementExecutor(String tableName) {
        TorqueStatementPeerImpl statementPeer = new TorqueStatementPeerImpl();
        statementPeer.init(this.getConfig(),tableName);
        return statementPeer;
    }

    @Override
    public void close(Connection connection) {
        Torque.closeConnection(connection);
    }

    @Override
    public void shutdown() throws Exception {
        Torque.shutdown();
    }

    @Override
    public boolean isClosed() {
        return !Torque.isInit();
    }

    /**
     * http://db.apache.org/torque/torque-4.0/documentation/orm-reference/initialisation-configuration.html
     * http://commons.apache.org/proper/commons-dbcp/configuration.html
     *
     * @param config
     * @return
     */
    private Configuration buildConfiguration(ConnectionConfig config){
        Configuration configuration = new BaseConfiguration();


        String databaseName = config.getDatabaseName();
        if(databaseName==null){
            LOG.warn(JdbcConstants.EAGLE_DATABASE+" is null, trying default database name as: eagle");
            databaseName = "eagle";
        }

        LOG.info("Using default database: "+databaseName+" (adapter: "+config.getAdapter()+")");

        configuration.addProperty("torque.database.default",config.getDatabaseName());

        // This factory uses the SharedDataSource available in the commons-dbcp package
        configuration.addProperty(String.format("torque.dsfactory.%s.factory",databaseName), DEFAULT_DATA_SOURCE_FACTORY_CLASS);

        // mysql, oracle, ...
        configuration.addProperty(String.format("torque.database.%s.adapter",databaseName),config.getAdapter());

        // "org.gjt.mm.mysql.Driver"
        configuration.addProperty(String.format("torque.dsfactory.%s.connection.driver",databaseName),config.getDriverClassName());

        configuration.addProperty(String.format("torque.dsfactory.%s.connection.url",databaseName),config.getConnectionUrl());
        configuration.addProperty(String.format("torque.dsfactory.%s.connection.user",databaseName),config.getUserName());
        configuration.addProperty(String.format("torque.dsfactory.%s.connection.password",databaseName),config.getPassword());
        configuration.addProperty(String.format("torque.dsfactory.%s.pool.maxActive",databaseName),Integer.toString(config.getConnectionMaxActive()));
//        configuration.addProperty(String.format("torque.dsfactory.%s.pool.minIdle",databaseName),Integer.toString(config.getConnectionMinIdle()));
//        configuration.addProperty(String.format("torque.dsfactory.%s.pool.initialSize",databaseName),Integer.toString(config.getConnectionInitialSize()));

        return configuration;
    }
}