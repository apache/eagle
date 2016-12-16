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


-- application framework metadata

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

-- eagle security module metadata

CREATE TABLE IF NOT EXISTS hdfs_sensitivity_entity (
  site varchar(20) NOT NULL,
  filedir varchar(100) NOT NULL,
  sensitivity_type varchar(20) DEFAULT NULL,
  primary key (site, filedir)
);

CREATE TABLE IF NOT EXISTS ip_securityzone (
  iphost varchar(100) NOT NULL,
  security_zone varchar(100) DEFAULT NULL,
  primary key (iphost)
);

CREATE TABLE IF NOT EXISTS hbase_sensitivity_entity (
  site varchar(20) NOT NULL,
  hbase_resource varchar(100) NOT NULL,
  sensitivity_type varchar(20) DEFAULT NULL,
  primary key (site, hbase_resource)
);

-- alert engine metadata

CREATE TABLE IF NOT EXISTS stream_cluster (
  id VARCHAR (50) PRIMARY KEY,
  content longtext DEFAULT NULL
);

CREATE TABLE IF NOT EXISTS stream_definition (
  id VARCHAR (50) PRIMARY KEY,
  content longtext DEFAULT NULL
);

CREATE TABLE IF NOT EXISTS kafka_tuple_metadata (
  id VARCHAR (50) PRIMARY KEY,
  content longtext DEFAULT NULL
);

CREATE TABLE IF NOT EXISTS policy_definition (
  id VARCHAR (50) PRIMARY KEY,
  content longtext DEFAULT NULL
);

CREATE TABLE IF NOT EXISTS publishment (
  id VARCHAR (50) PRIMARY KEY,
  content longtext DEFAULT NULL
);

CREATE TABLE IF NOT EXISTS schedule_state (
  id VARCHAR (50) PRIMARY KEY,
  content longtext DEFAULT NULL
);

CREATE TABLE IF NOT EXISTS policy_assignment (
  id VARCHAR (50) PRIMARY KEY,
  content longtext DEFAULT NULL
);

CREATE TABLE IF NOT EXISTS topology (
  id VARCHAR (50) PRIMARY KEY,
  content longtext DEFAULT NULL
);

CREATE TABLE IF NOT EXISTS publishment_type (
  id VARCHAR (50) PRIMARY KEY,
  content longtext DEFAULT NULL
);

CREATE TABLE IF NOT EXISTS policy_publishment (
  policyId VARCHAR(50),
  publishmentName VARCHAR(50),
  PRIMARY KEY(policyId, publishmentName),
  CONSTRAINT `policy_id_fk` FOREIGN KEY (`policyId`) REFERENCES `policy_definition` (`id`) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `publishment_id_fk` FOREIGN KEY (`publishmentName`) REFERENCES `publishment` (`id`) ON DELETE CASCADE ON UPDATE CASCADE
);

CREATE TABLE IF NOT EXISTS alert_event (
  alertId VARCHAR (50) PRIMARY KEY,
  siteId VARCHAR (50) DEFAULT NULL,
  appIds VARCHAR (255) DEFAULT NULL,
  policyId VARCHAR (50) DEFAULT NULL,
  alertTimestamp bigint(20) DEFAULT NULL,
  policyValue mediumtext DEFAULT NULL,
  alertData mediumtext DEFAULT NULL
);

INSERT INTO publishment_type(id, content) VALUES
('Kafka', '{"name":"Kafka","type":"org.apache.eagle.alert.engine.publisher.impl.AlertKafkaPublisher","description":null,"fields":[{"name":"kafka_broker","value":"sandbox.hortonworks.com:6667"},{"name":"topic"}]}'),
('Email', '{"name":"Email","type":"org.apache.eagle.alert.engine.publisher.impl.AlertEmailPublisher","description":null,"fields":[{"name":"subject"},{"name":"sender"}, {"name":"recipients"}]}'),
('Slack', '{"name":"Slack","type":"org.apache.eagle.alert.engine.publisher.impl.AlertSlackPublisher","description":null,"fields":[{"name":"token"},{"name":"channels"}, {"name":"severitys"}, {"name":"urltemplate"}]}'),
('Storage', '{"name":"Storage","type":"org.apache.eagle.alert.app.AlertEagleStorePlugin","description":null,"fields":[]}');
