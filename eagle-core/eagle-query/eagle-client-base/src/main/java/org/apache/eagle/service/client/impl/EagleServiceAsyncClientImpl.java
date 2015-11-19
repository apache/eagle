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
package org.apache.eagle.service.client.impl;

import org.apache.eagle.log.base.taggedlog.TaggedLogAPIEntity;
import org.apache.eagle.log.entity.GenericServiceAPIResponseEntity;
import org.apache.eagle.service.client.EagleServiceAsyncClient;
import org.apache.eagle.service.client.IEagleServiceClient;
import org.apache.eagle.service.client.EagleServiceClientException;
import org.apache.eagle.service.client.EagleServiceSingleEntityQueryRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

public class EagleServiceAsyncClientImpl implements EagleServiceAsyncClient {
    private final IEagleServiceClient client;
    private final static Logger LOG = LoggerFactory.getLogger(EagleServiceAsyncClientImpl.class);

    public EagleServiceAsyncClientImpl(IEagleServiceClient client) {
        this.client = client;
    }

    @Override
    public void close() throws IOException {
        if(LOG.isDebugEnabled()) LOG.debug("Executor service is shutting down");
        this.client.getJerseyClient().getExecutorService().shutdown();
    }

    @Override
    public <E extends TaggedLogAPIEntity> Future<GenericServiceAPIResponseEntity<String>> create(final List<E> entities, final String serviceName) throws IOException, EagleServiceClientException {
        return this.client.getJerseyClient().getExecutorService().submit(new Callable<GenericServiceAPIResponseEntity<String>>() {
            @Override
            public GenericServiceAPIResponseEntity<String> call() throws Exception {
                return client.create(entities,serviceName);
            }
        });
    }

    @Override
    public <E extends TaggedLogAPIEntity> Future<GenericServiceAPIResponseEntity<String>> create(final List<E> entities, final Class<E> entityClass) throws IOException, EagleServiceClientException {
        return this.client.getJerseyClient().getExecutorService().submit(new Callable<GenericServiceAPIResponseEntity<String>>() {
            @Override
            public GenericServiceAPIResponseEntity<String> call() throws Exception {
                return client.create(entities,entityClass);
            }
        });
    }

    @Override
    public <E extends TaggedLogAPIEntity> Future<GenericServiceAPIResponseEntity<String>> create(final List<E> entities) throws IOException, EagleServiceClientException {
        return this.client.getJerseyClient().getExecutorService().submit(new Callable<GenericServiceAPIResponseEntity<String>>() {
            @Override
            public GenericServiceAPIResponseEntity<String> call() throws Exception {
                return client.create(entities);
            }
        });
    }

    @Override
    public <E extends TaggedLogAPIEntity> Future<GenericServiceAPIResponseEntity<String>> delete(final List<E> entities) throws IOException, EagleServiceClientException {
        return this.client.getJerseyClient().getExecutorService().submit(new Callable<GenericServiceAPIResponseEntity<String>>() {
            @Override
            public GenericServiceAPIResponseEntity<String> call() throws Exception {
                return client.delete(entities);
            }
        });
    }

    @Override
    public <E extends TaggedLogAPIEntity> Future<GenericServiceAPIResponseEntity<String>> delete(final List<E> entities, final String serviceName) throws IOException, EagleServiceClientException {
        return this.client.getJerseyClient().getExecutorService().submit(new Callable<GenericServiceAPIResponseEntity<String>>() {
            @Override
            public GenericServiceAPIResponseEntity<String> call() throws Exception {
                return client.delete(entities, serviceName);
            }
        });
    }

    @Override
    public <E extends TaggedLogAPIEntity> Future<GenericServiceAPIResponseEntity<String>> delete(final List<E> entities, final Class<E> entityClass) throws IOException, EagleServiceClientException {
        return this.client.getJerseyClient().getExecutorService().submit(new Callable<GenericServiceAPIResponseEntity<String>>() {
            @Override
            public GenericServiceAPIResponseEntity<String> call() throws Exception {
                return client.create(entities,entityClass);
            }
        });
    }

    @Override
    public Future<GenericServiceAPIResponseEntity<String>> delete(final EagleServiceSingleEntityQueryRequest request) throws EagleServiceClientException, IOException {
        return this.client.getJerseyClient().getExecutorService().submit(new Callable<GenericServiceAPIResponseEntity<String>>() {
            @Override
            public GenericServiceAPIResponseEntity<String> call() throws Exception {
                return client.delete(request);
            }
        });
    }

    @Override
    public <E extends TaggedLogAPIEntity> Future<GenericServiceAPIResponseEntity<String>> update(final List<E> entities) throws IOException, EagleServiceClientException {
        return this.client.getJerseyClient().getExecutorService().submit(new Callable<GenericServiceAPIResponseEntity<String>>() {
            @Override
            public GenericServiceAPIResponseEntity<String> call() throws Exception {
                return client.update(entities);
            }
        });
    }

    @Override
    public <E extends TaggedLogAPIEntity> Future<GenericServiceAPIResponseEntity<String>> update(final List<E> entities, final String serviceName) throws IOException, EagleServiceClientException {
        return this.client.getJerseyClient().getExecutorService().submit(new Callable<GenericServiceAPIResponseEntity<String>>() {
            @Override
            public GenericServiceAPIResponseEntity<String> call() throws Exception {
                return client.update(entities, serviceName);
            }
        });
    }

    @Override
    public <E extends TaggedLogAPIEntity> Future<GenericServiceAPIResponseEntity<String>> update(final List<E> entities, final Class<E> entityClass) throws IOException, EagleServiceClientException {
        return this.client.getJerseyClient().getExecutorService().submit(new Callable<GenericServiceAPIResponseEntity<String>>() {
            @Override
            public GenericServiceAPIResponseEntity<String> call() throws Exception {
                return client.update(entities, entityClass);
            }
        });
    }

    @Override
    public <E> Future<GenericServiceAPIResponseEntity<String>> search(final EagleServiceSingleEntityQueryRequest request) throws EagleServiceClientException {
        return this.client.getJerseyClient().getExecutorService().submit(new Callable<GenericServiceAPIResponseEntity<String>>() {
            @Override
            public GenericServiceAPIResponseEntity<String> call() throws Exception {
                return client.search(request);
            }
        });
    }
}