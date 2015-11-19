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
import com.sun.jersey.api.client.Client;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

public interface IEagleServiceClient extends IEagleServiceRequestBuilder, Closeable {

    Client getJerseyClient();

    IEagleServiceClient silence(boolean silence);

    /**
     *
     * @param <E>
     * @param entities
     * @param serviceName
     * @return
     */
    <E extends TaggedLogAPIEntity> GenericServiceAPIResponseEntity<String> create(List<E> entities,String serviceName) throws IOException, EagleServiceClientException;

    /**
     *
     * @param entities
     * @param entityClass
     * @param <E>
     * @return
     */
    <E extends TaggedLogAPIEntity> GenericServiceAPIResponseEntity<String> create(List<E> entities,Class<E> entityClass) throws IOException, EagleServiceClientException;

    /**
     *
     * @param entities
     * @param <E>
     * @return
     */
    <E extends TaggedLogAPIEntity> GenericServiceAPIResponseEntity<String> create(List<E> entities) throws IOException, EagleServiceClientException;

    /**
     *
     * @param entities
     * @param <E>
     * @return
     */
    <E extends TaggedLogAPIEntity> GenericServiceAPIResponseEntity<String> delete(List<E> entities) throws IOException, EagleServiceClientException;

    /**
     *
     * @param entities
     * @param <E>
     * @return
     */
    <E extends TaggedLogAPIEntity> GenericServiceAPIResponseEntity<String> delete(List<E> entities,String serviceName) throws IOException, EagleServiceClientException;

    /**
     *
     * @param entities
     * @param <E>
     * @return
     */
    <E extends TaggedLogAPIEntity> GenericServiceAPIResponseEntity<String> delete(List<E> entities,Class<E> entityClass) throws IOException, EagleServiceClientException;

    /**
     *
     * @param request
     * @return
     */
    GenericServiceAPIResponseEntity<String> delete(EagleServiceSingleEntityQueryRequest request) throws EagleServiceClientException, IOException;

    /**
     *
     * @param ids
     * @param serviceName
     * @return
     * @throws EagleServiceClientException
     * @throws IOException
     */
    GenericServiceAPIResponseEntity<String> deleteById(List<String> ids,String serviceName) throws EagleServiceClientException, IOException;

    /**
     * @param entities
     * @param <E>
     * @return
     */
    <E extends TaggedLogAPIEntity> GenericServiceAPIResponseEntity<String> update(List<E> entities) throws IOException, EagleServiceClientException;

    /**
     *
     * @param entities
     * @param <E>
     * @return
     */
    <E extends TaggedLogAPIEntity> GenericServiceAPIResponseEntity<String> update(List<E> entities,String serviceName) throws IOException, EagleServiceClientException;

    /**
     *
     * @param entities
     * @param <E>
     * @return
     */
    <E extends TaggedLogAPIEntity> GenericServiceAPIResponseEntity<String> update(List<E> entities,Class<E> entityClass) throws IOException, EagleServiceClientException;

    /**
     *
     * @param request
     * @return
     */
    <E extends Object> GenericServiceAPIResponseEntity<E> search(EagleServiceSingleEntityQueryRequest request) throws EagleServiceClientException;
}