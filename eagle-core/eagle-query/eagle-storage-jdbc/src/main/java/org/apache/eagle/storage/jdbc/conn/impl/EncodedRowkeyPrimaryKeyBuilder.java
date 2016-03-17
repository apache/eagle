/**
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
package org.apache.eagle.storage.jdbc.conn.impl;


import org.apache.eagle.common.EagleBase64Wrapper;
import org.apache.eagle.log.base.taggedlog.TaggedLogAPIEntity;
import org.apache.eagle.log.entity.meta.EntityDefinition;
import org.apache.eagle.log.entity.meta.EntityDefinitionManager;
import org.apache.eagle.log.entity.old.RowkeyHelper;
import org.apache.eagle.storage.jdbc.conn.PrimaryKeyBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EncodedRowkeyPrimaryKeyBuilder implements PrimaryKeyBuilder<String> {
    private static final Logger LOG = LoggerFactory.getLogger(EncodedRowkeyPrimaryKeyBuilder.class);

    @Override
    public <T> String build(T t) {
        if(t == null) return null;

        try {
            EntityDefinition entityDefinition
                    = EntityDefinitionManager.getEntityDefinitionByEntityClass((Class<? extends TaggedLogAPIEntity>) t.getClass());
            return EagleBase64Wrapper.encodeByteArray2URLSafeString(RowkeyHelper.getRowkey((TaggedLogAPIEntity) t,entityDefinition));
        } catch (Exception e) {
            LOG.error("Got error to build rowKey for {}",t,e);
            throw new RuntimeException("Got error to build rowKey",e);
        }
    }
}