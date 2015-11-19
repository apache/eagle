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
/**
 * <h1>Eagle Storage Extension - JDBC</h1>
 *
 * <h2>Configuration</h2>
 * <ul>
 *  <li>
 *     eagle.storage.type: <code>jdbc</code>
 *  </li>
 *  <li>
 *     eagle.storage.adapter:
 *      <code>mysql</code>
 *      <code>mysql</code>
 *      <code>oracle</code>
 *      <code>postgres</code>
 *      <code>mssql</code>
 *      <code>hsqldb</code>
 *      <code>derby</code>
 *  </li>
 *  <li>eagle.storage.username</li>
 *  <li>eagle.storage.password</li>
 *  <li>eagle.storage.database</li>
 *  <li>eagle.storage.connection.url</li>
 *  <li>eagle.storage.connection.props</li>
 *  <li>eagle.storage.driver.class</li>
 *  <li>eagle.storage.connection.max</li>
 * </ul>
 *
 * Sample:
 * <pre>
 *   eagle.storage.type=jdbc
 *   eagle.storage.adapter=mysql
 *   eagle.storage.username=eagle
 *   eagle.storage.password=eagle
 *   eagle.storage.database=eagle
 *   eagle.storage.connection.url=jdbc:mysql://localhost:3306/eagle
 *   eagle.storage.connection.props=encoding=UTF-8
 *   eagle.storage.driver.class=com.mysql.jdbc.Driver
 *   eagle.storage.connection.max=8
 * </pre>
 *
 * <h2>Rowkey</h2>
 *
 * We simply use UUID as row key of entities and use the key as the primary key named "uuid" in RDBMS table schema
 *
 * <h2>Features</h2>
 * <ul>
 *  <li>Support basic entity operation like  CREATE, READ, UPDATE and DELETE</li>
 *  <li>Support flatten aggregation query</li>
 *  <li>Support customized entity field type (JdbcEntityDefinitionManager#registerJdbcSerDeser) </li>
 * </ul>
 *
 * <h2>Dependencies</h2>
 * <ul>
 *     <li>Apache DB - Torque: http://db.apache.org/torque/torque-4.0/index.html</li>
 *     <li>Apache DB - DdlUtils: https://db.apache.org/ddlutils/</li>
 * </ul>
 *
 * <h2>TO-DO</h2>
 * <ul>
 *     <li>Support time-series based aggregation</li>
 *     <li>Investigate why writing performance becomes slower as records count in table increases</li>
 *     <li>Implement batch insert in JdbcEntityWriterImpl</li>
 *     <li>Implement DDL Management to generate default table schema DDL according entity definition</li>
 * </ul>
 *
 * @since 3/26/15
 */
package org.apache.eagle.storage.jdbc;