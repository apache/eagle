/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.eagle.jpm.analyzer.meta.impl.orm;

import org.apache.eagle.common.function.ThrowableFunction;
import org.apache.eagle.jpm.analyzer.meta.model.JobMetaEntity;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;


public class RelationToJobMetaEntity implements ThrowableFunction<ResultSet, JobMetaEntity, SQLException> {
    private static final Logger LOG = LoggerFactory.getLogger(RelationToJobMetaEntity.class);

    @Override
    public JobMetaEntity apply(ResultSet resultSet) throws SQLException {
        JobMetaEntity jobMetaEntity = new JobMetaEntity();
        jobMetaEntity.setUuid(resultSet.getString(1));
        jobMetaEntity.setJobDefId(resultSet.getString(2));
        jobMetaEntity.setSiteId(resultSet.getString(3));
        jobMetaEntity.setConfiguration(parse(resultSet.getString(4)));
        jobMetaEntity.setEvaluators(new ArrayList<>());
        try {
            JSONArray jsonArray = new JSONArray(resultSet.getString(5));
            for (int i = 0; i < jsonArray.length(); ++i) {
                jobMetaEntity.getEvaluators().add(jsonArray.getString(i));
            }
        } catch (Exception e) {
            LOG.warn("{}", e);
        }
        jobMetaEntity.setCreatedTime(resultSet.getLong(6));
        jobMetaEntity.setModifiedTime(resultSet.getLong(7));

        return jobMetaEntity;
    }

    private Map<String, Object> parse(String field) {
        Map<String, Object> items = new java.util.HashMap<>();
        try {
            JSONObject jsonObject = new JSONObject(field);

            Iterator<String> keyItemItr = jsonObject.keys();
            while (keyItemItr.hasNext()) {
                String itemKey = keyItemItr.next();
                items.put(itemKey, jsonObject.get(itemKey));
            }

        } catch (Exception e) {
            LOG.warn("{}", e);
        }

        return items;
    }
}
