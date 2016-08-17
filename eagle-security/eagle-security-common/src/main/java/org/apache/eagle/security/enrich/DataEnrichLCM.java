/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.eagle.security.enrich;

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;

/**
 * Since 8/16/16.
 * Data enrichment lifecycle methods
 * @param <T> entity type, this entity will be put into cache
 * @param <K> cache key
 */
public interface DataEnrichLCM<T, K> extends Serializable{
    /**
     * load all external data for real time enrichment
     *
     * @return
     */
    Collection<T> loadExternal();

    /**
     * get cache key from one entity
     *
     * @param entity
     * @return
     */
    K getCacheKey(T entity);
}