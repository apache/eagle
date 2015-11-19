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
package org.apache.eagle.service.security.hive.dao;

public class HiveMetadataDAOFactory {
    public HiveMetadataDAO getHiveMetadataDAO(HiveMetadataAccessConfig config){
        if(config.getAccessType() == null) throw new BadHiveMetadataAccessConfigException("access Type is null, options: [hiveserver2_jdbc,metastoredb_jdbc]");
        if(config.getAccessType().equals(HiveMetadataAccessType.hiveserver2_jdbc.name())){
            return new HiveMetadataByHiveServer2DAOImpl(config);
        }else if(config.getAccessType().equals(HiveMetadataAccessType.metastoredb_jdbc.name())){
            return new HiveMetadataByMetastoreDBDAOImpl(config);
        }else{
            throw new BadHiveMetadataAccessConfigException("Hive metadata access type has to be one of hiveserver2_jdbc and metastoredb_jdbc");
        }
    }
}
