/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *    http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */
package org.apache.eagle.security.auditlog;

import org.apache.eagle.alert.dao.AlertDefinitionDAOImpl;
import org.apache.eagle.security.hdfs.entity.HdfsUserCommandPatternEntity;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.List;

/**
 * get HdfsUserCommandPattern from eagle database
 */
public class HdfsUserCommandPatternByFileImpl implements HdfsUserCommandPatternDAO {
    private final Logger LOG = LoggerFactory.getLogger(HdfsUserCommandPatternByFileImpl.class);

    @Override
    public List<HdfsUserCommandPatternEntity> findAllPatterns() throws Exception{
        ObjectMapper mapper = new ObjectMapper();

        InputStream is = this.getClass().getResourceAsStream("/hdfsUserCommandPattern.json");
        try{
            Object o = mapper.readValue(is, new TypeReference<List<HdfsUserCommandPatternEntity>>(){});
            return (List<HdfsUserCommandPatternEntity>)o;
        }catch(Exception ex){
            LOG.error("can't read hdfsUserCommandPattern.json", ex);
            throw ex;
        }
    }
}
