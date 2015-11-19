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

import org.apache.eagle.storage.operation.CompiledQuery;

import java.util.List;

/**
 * @since 3/27/15
 */
public interface JdbcEntityReader {
    /**
     *
     * @param query query
     * @param <E> entity type
     * @return result entities list
     * @throws Exception
     */
    public <E extends Object> List<E> query(CompiledQuery query) throws Exception;

    public <E extends Object> List<E> query(List<String> pk) throws Exception;

    /**
     *
     * @return firstTimestamp
     */
    public Long getResultFirstTimestamp();

    /**
     *
     * @return lastTimeStamp
     */
    public Long getResultLastTimestamp();
}
