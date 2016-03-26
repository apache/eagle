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
 *
 */

CREATE TABLE UNITTEST_TESTTSENTITY (
  uuid varchar(255) NOT NULL,
  field1 int DEFAULT NULL,
  field2 int DEFAULT NULL,
  field3 BIGINT DEFAULT NULL,
  field4 BIGINT DEFAULT NULL,
  field5 double DEFAULT NULL,
  field6 double DEFAULT NULL,
  field7 varchar(255) DEFAULT NULL,
  cluster varchar(255) DEFAULT NULL,
  datacenter varchar(255) DEFAULT NULL,
  random_tag VARCHAR(255) DEFAULT NULL,
  timestamp BIGINT DEFAULT NULL,
  jobid varchar(255) DEFAULT NULL,
  PRIMARY KEY (uuid)
)