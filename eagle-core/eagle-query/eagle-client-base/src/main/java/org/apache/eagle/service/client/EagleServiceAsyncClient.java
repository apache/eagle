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
package org.apache.eagle.service.client;

import org.apache.eagle.log.base.taggedlog.TaggedLogAPIEntity;
import org.apache.eagle.log.entity.GenericServiceAPIResponseEntity;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.Future;

/**
 * @see IEagleServiceClient
 */
public interface EagleServiceAsyncClient extends Closeable{
    /**
     *
     * @param <E>
     * @param entities
     * @param serviceName
     * @return
     */
    <E extends TaggedLogAPIEntity> Future<GenericServiceAPIResponseEntity<String>> create(final List<E> entities,final String serviceName) throws IOException, EagleServiceClientException;

    /**
     *
     * @param entities
     * @param entityClass
     * @param <E>
     * @return
     */
    <E extends TaggedLogAPIEntity> Future<GenericServiceAPIResponseEntity<String>> create(final List<E> entities,final Class<E> entityClass) throws IOException, EagleServiceClientException;

    /**
     *
     * @param entities
     * @param <E>
     * @return
     */
    <E extends TaggedLogAPIEntity> Future<GenericServiceAPIResponseEntity<String>> create(final List<E> entities) throws IOException, EagleServiceClientException;

    /**
     *
     * @param entities
     * @param <E>
     * @return
     */
    <E extends TaggedLogAPIEntity> Future<GenericServiceAPIResponseEntity<String>> delete(final List<E> entities) throws IOException, EagleServiceClientException;

    /**
     *
     * @param entities
     * @param <E>
     * @return
     */
    <E extends TaggedLogAPIEntity> Future<GenericServiceAPIResponseEntity<String>> delete(final List<E> entities,final String serviceName) throws IOException, EagleServiceClientException;

    /**
     *
     * @param entities
     * @param <E>
     * @return
     */
    <E extends TaggedLogAPIEntity> Future<GenericServiceAPIResponseEntity<String>> delete(final List<E> entities,final Class<E> entityClass) throws IOException, EagleServiceClientException;

    /**
     *
     * @param request
     * @return
     */
    Future<GenericServiceAPIResponseEntity<String>> delete(final EagleServiceSingleEntityQueryRequest request) throws EagleServiceClientException, IOException;

    /**
     * @param entities
     * @param <E>
     * @return
     */
    <E extends TaggedLogAPIEntity> Future<GenericServiceAPIResponseEntity<String>> update(final List<E> entities) throws IOException, EagleServiceClientException;

    /**
     *
     * @param entities
     * @param <E>
     * @return
     */
    <E extends TaggedLogAPIEntity> Future<GenericServiceAPIResponseEntity<String>> update(final List<E> entities,final String serviceName) throws IOException, EagleServiceClientException;

    /**
     *
     * @param entities
     * @param <E>
     * @return
     */
    <E extends TaggedLogAPIEntity> Future<GenericServiceAPIResponseEntity<String>> update(final List<E> entities,final Class<E> entityClass) throws IOException, EagleServiceClientException;

    /**
     *
     * @param request
     * @return
     */
    <E extends Object> Future<GenericServiceAPIResponseEntity<String>> search(final EagleServiceSingleEntityQueryRequest request) throws EagleServiceClientException;
}
