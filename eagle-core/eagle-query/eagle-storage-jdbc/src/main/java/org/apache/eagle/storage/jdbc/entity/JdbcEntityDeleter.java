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
package org.apache.eagle.storage.jdbc.entity;

import org.apache.eagle.log.base.taggedlog.TaggedLogAPIEntity;
import org.apache.eagle.storage.operation.CompiledQuery;

import java.util.List;

public interface JdbcEntityDeleter<E extends TaggedLogAPIEntity> {
    /**
     * delete.
     * @param entities
     * @return
     */
    public int delete(List<E> entities) throws Exception;

    /**
     * delete by ids.
     * @param ids
     * @return
     */
    public int deleteByIds(List<String> ids) throws Exception;

    /**
     * delete by query.
     * @param query
     * @return
     */
    public int deleteByQuery(CompiledQuery query) throws Exception;
}