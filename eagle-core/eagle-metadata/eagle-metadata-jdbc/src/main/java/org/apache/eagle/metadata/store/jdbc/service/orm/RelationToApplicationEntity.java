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

package org.apache.eagle.metadata.store.jdbc.service.orm;

import org.apache.eagle.common.function.ThrowableFunction;
import org.apache.eagle.metadata.model.ApplicationDesc;
import org.apache.eagle.metadata.model.ApplicationEntity;
import org.apache.eagle.metadata.model.SiteEntity;
import org.apache.eagle.metadata.store.jdbc.service.ApplicationEntityServiceJDBCImpl;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.Map;


public class RelationToApplicationEntity implements ThrowableFunction<ResultSet, ApplicationEntity, SQLException> {

    private static final Logger LOG = LoggerFactory.getLogger(RelationToApplicationEntity.class);
    @Override
    public ApplicationEntity apply(ResultSet resultSet) throws SQLException {

        ApplicationDesc applicationDesc = new ApplicationDesc();
        String appType = resultSet.getString(4);
        applicationDesc.setType(appType);

        SiteEntity siteEntity = new SiteEntity();
        siteEntity.setUuid(resultSet.getString(11));
        siteEntity.setSiteId(resultSet.getString(12));
        siteEntity.setSiteName(resultSet.getString(13));
        siteEntity.setDescription(resultSet.getString(14));
        siteEntity.setCreatedTime(resultSet.getLong(15));
        siteEntity.setModifiedTime(resultSet.getLong(16));


        ApplicationEntity resultEntity = new ApplicationEntity();
        resultEntity.setUuid(resultSet.getString(1));
        resultEntity.setAppId(resultSet.getString(2));
        resultEntity.setSite(siteEntity);
        resultEntity.setDescriptor(applicationDesc);
        resultEntity.setMode(ApplicationEntity.Mode.valueOf(resultSet.getString(5)));
        resultEntity.setJarPath(resultSet.getString(6));
        resultEntity.setStatus(ApplicationEntity.Status.valueOf(resultSet.getString(7)));
        try {
            JSONObject jsonObject = new JSONObject(resultSet.getString(8));
            Map<String, Object> items = new java.util.HashMap<>();
            Iterator<String> keyItemItr = jsonObject.keys();
            while (keyItemItr.hasNext()) {
                String itemKey = keyItemItr.next();
                items.put(itemKey, jsonObject.get(itemKey));
            }
            resultEntity.setConfiguration(items);
        } catch (Exception e) {
            LOG.warn("{}", e);
        }
        resultEntity.setCreatedTime(resultSet.getLong(9));
        resultEntity.setModifiedTime(resultSet.getLong(10));

        return resultEntity;
    }
}
