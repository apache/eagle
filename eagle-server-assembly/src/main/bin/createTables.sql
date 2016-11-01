-- /*
--  * Licensed to the Apache Software Foundation (ASF) under one or more
--  * contributor license agreements.  See the NOTICE file distributed with
--  * this work for additional information regarding copyright ownership.
--  * The ASF licenses this file to You under the Apache License, Version 2.0
--  * (the "License"); you may not use this file except in compliance with
--  * the License.  You may obtain a copy of the License at
--  *
--  *    http://www.apache.org/licenses/LICENSE-2.0
--  *
--  * Unless required by applicable law or agreed to in writing, software
--  * distributed under the License is distributed on an "AS IS" BASIS,
--  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
--  * See the License for the specific language governing permissions and
--  * limitations under the License.
--  *
--  */

CREATE TABLE IF NOT EXISTS applications (
  uuid varchar(50) PRIMARY KEY,
  appid varchar(100) DEFAULT NULL,
  siteid varchar(100) DEFAULT NULL,
  apptype varchar(30) DEFAULT NULL,
  appmode varchar(10) DEFAULT NULL,
  jarpath varchar(255) DEFAULT NULL,
  appstatus  varchar(20) DEFAULT NULL,
  configuration mediumtext DEFAULT NULL,
  context mediumtext DEFAULT NULL,
  createdtime bigint(20) DEFAULT NULL,
  modifiedtime  bigint(20) DEFAULT NULL
);

CREATE TABLE IF NOT EXISTS sites (
  uuid varchar(50) PRIMARY KEY,
  siteid varchar(100) DEFAULT NULL,
  sitename varchar(100) DEFAULT NULL,
  description varchar(255) DEFAULT NULL,
  createdtime bigint(20) DEFAULT NULL,
  modifiedtime  bigint(20) DEFAULT NULL,
  UNIQUE (siteid)
);

CREATE TABLE IF NOT EXISTS hdfs_sensitivity_entity (
  site varchar(20) DEFAULT NULL,
  filedir varchar(100) DEFAULT NULL,
  sensitivity_type varchar(20) DEFAULT NULL,
  primary key (site, filedir)
);

CREATE TABLE IF NOT EXISTS ip_securityzone (
  iphost varchar(100) DEFAULT NULL,
  security_zone varchar(100) DEFAULT NULL,
  primary key (iphost)
);

CREATE TABLE IF NOT EXISTS hbase_sensitivity_entity (
  site varchar(20) DEFAULT NULL,
  hbase_resource varchar(100) DEFAULT NULL,
  sensitivity_type varchar(20) DEFAULT NULL,
  primary key (site, hbase_resource)
);