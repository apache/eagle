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
import org.apache.eagle.service.client.IEagleServiceClient;
import org.apache.eagle.service.client.EagleServiceClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

public class BatchSender implements Closeable {
    private final static Logger LOG = LoggerFactory.getLogger(BatchSender.class);
    private final List<TaggedLogAPIEntity> entityBucket;
    private final IEagleServiceClient client;

    protected int getBatchSize() {
        return batchSize;
    }

    protected void setBatchSize(int batchSize) {
        if(batchSize<0) throw new IllegalArgumentException("batch size should be "+batchSize);
        this.batchSize = batchSize;
    }

    private int batchSize;

    public BatchSender(IEagleServiceClient client, int batchSize){
        this.setBatchSize(batchSize);
        this.client = client;
        this.entityBucket = new LinkedList<TaggedLogAPIEntity>();
    }

    public BatchSender send(TaggedLogAPIEntity entity) throws IOException, EagleServiceClientException {
        this.entityBucket.add(entity);
        if(this.entityBucket.size()>=this.batchSize){
            flush();
        }
        return this;
    }

    public BatchSender send(List<TaggedLogAPIEntity> entities) throws IOException, EagleServiceClientException {
        this.entityBucket.addAll(entities);
        if(this.entityBucket.size()>= this.batchSize){
            flush();
        }
        return this;
    }

    public void flush() throws IOException, EagleServiceClientException {
        if(this.entityBucket.size() == 0 && LOG.isDebugEnabled()){
            LOG.debug("No entities to flush");
            return;
        }

        LOG.info("Writing "+this.entityBucket.size()+" entities");
        GenericServiceAPIResponseEntity<String> response = this.client.create(this.entityBucket);
        if(!response.isSuccess()){
            LOG.error("Got service exception: "+response.getException());
            throw new IOException("Service exception"+response.getException());
        }else{
            this.entityBucket.clear();
        }
    }

    @Override
    public void close() throws IOException {
        try {
            flush();
        } catch (EagleServiceClientException e) {
            throw new IOException(e);
        }
    }
}