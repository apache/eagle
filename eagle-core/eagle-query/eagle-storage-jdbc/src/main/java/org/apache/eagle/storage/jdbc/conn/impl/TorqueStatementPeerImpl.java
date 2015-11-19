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

import org.apache.eagle.storage.jdbc.conn.ConnectionConfig;
import org.apache.eagle.storage.jdbc.conn.PrimaryKeyBuilder;
import org.apache.eagle.storage.jdbc.conn.StatementExecutor;
import org.apache.torque.adapter.IDMethod;
import org.apache.torque.map.TableMap;
import org.apache.torque.util.BasePeerImpl;

/**
 * @since 3/29/15
 */
public class TorqueStatementPeerImpl<T> implements StatementExecutor {
    private BasePeerImpl<T> basePeer;
    public TorqueStatementPeerImpl(){
        this.basePeer = new BasePeerImpl<T>();
    }

    @Override
    public void init(ConnectionConfig config) {
        this.basePeer.setDatabaseName(config.getDatabaseName());
    }

    @Override
    public void init(ConnectionConfig config, String tableName) {
        this.basePeer.setDatabaseName(config.getDatabaseName());
        TableMap tableMap = new TableMap(tableName,null,null);
        tableMap.setPrimaryKeyMethodInfo(IDMethod.NO_ID_METHOD);
        this.basePeer.setTableMap(tableMap);
    }

    private static PrimaryKeyBuilder<String> _primaryKeyBuilderInstance = new UUIDPrimaryKeyBuilder();;
    @Override
    public PrimaryKeyBuilder<String> getPrimaryKeyBuilder() {
        return _primaryKeyBuilderInstance;
    }

    public BasePeerImpl<T> delegate(){
        return this.basePeer;
    }
}