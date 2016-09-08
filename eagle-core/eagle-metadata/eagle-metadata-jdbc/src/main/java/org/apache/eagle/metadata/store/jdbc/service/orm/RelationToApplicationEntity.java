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

import java.sql.ResultSet;
import java.sql.SQLException;


public class RelationToApplicationEntity implements ThrowableFunction<ResultSet, ApplicationEntity, SQLException> {
    @Override
    public ApplicationEntity apply(ResultSet resultSet) throws SQLException {

        ApplicationDesc applicationDesc = new ApplicationDesc();
        String appType = resultSet.getString(4);
        applicationDesc.setType(appType);

        SiteEntity siteEntity = new SiteEntity();
        siteEntity.setUuid(resultSet.getString(10));
        siteEntity.setSiteId(resultSet.getString(11));
        siteEntity.setSiteName(resultSet.getString(12));
        siteEntity.setDescription(resultSet.getString(13));
        siteEntity.setCreatedTime(resultSet.getLong(14));
        siteEntity.setModifiedTime(resultSet.getLong(15));


        ApplicationEntity resultEntity = new ApplicationEntity();
        resultEntity.setUuid(resultSet.getString(1));
        resultEntity.setAppId(resultSet.getString(2));
        resultEntity.setSite(siteEntity);
        resultEntity.setDescriptor(applicationDesc);
        resultEntity.setMode(ApplicationEntity.Mode.valueOf(resultSet.getString(5)));
        resultEntity.setJarPath(resultSet.getString(6));
        resultEntity.setStatus(ApplicationEntity.Status.valueOf(resultSet.getString(7)));
        resultEntity.setCreatedTime(resultSet.getLong(8));
        resultEntity.setModifiedTime(resultSet.getLong(9));

        return resultEntity;
    }
}
