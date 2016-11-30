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

CREATE TABLE IF NOT EXISTS stream_cluster (
  id VARCHAR (50) PRIMARY KEY,
  content longtext DEFAULT NULL
);

CREATE TABLE IF NOT EXISTS stream_definition (
  id VARCHAR (50) PRIMARY KEY,
  content longtext DEFAULT NULL
);

CREATE TABLE IF NOT EXISTS Kafka_tuple_metadata (
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