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
import org.apache.eagle.service.client.EagleServiceClientException;
import org.apache.eagle.service.client.IEagleServiceClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.SynchronousQueue;

public class ConcurrentSender implements Closeable{
    private final int parallelNum;
    private final IEagleServiceClient client;
    private final SynchronousQueue<TaggedLogAPIEntity> queue;
    private final List<Handler> handlers;
    private int batchSize = 1000;
    private long batchInterval = 3 * 1000;
    private boolean isStarted = false;

    private final static Logger LOG = LoggerFactory.getLogger(ConcurrentSender.class);

    public ConcurrentSender(IEagleServiceClient client, int parallelNum) {
        this.parallelNum = parallelNum;
        this.client = client;
        this.queue= new SynchronousQueue<TaggedLogAPIEntity>();
        this.handlers = Collections.synchronizedList(new LinkedList<Handler>());
    }

    public void start(){
        if(!this.isStarted) {
            LOG.info("Starting with handlers = " + this.parallelNum + ", batchSize = " + this.batchSize + ", batchInterval (ms) = " + this.batchInterval);

            for (int i = 0; i < this.parallelNum; i++) {
                Handler handler = new Handler(this.queue, this.client, this.batchSize, this.batchInterval);

                Thread thread = new Thread(handler);
                thread.setDaemon(true);
                thread.setName("Sender-" + i);
                this.handlers.add(handler);
                thread.start();
            }

            this.isStarted = true;
        }else{
            LOG.warn("Already started");
        }
    }

    public ConcurrentSender batchSize(int batchSize){
        this.batchSize = batchSize;
        return this;
    }

    public ConcurrentSender batchInterval(long batchInterval){
        this.batchInterval = batchInterval;
        return this;
    }

    public ConcurrentSender send(final List<? extends TaggedLogAPIEntity> entities) throws InterruptedException {
        for(TaggedLogAPIEntity entity:entities){
            this.send(entity);
        }
        return this;
    }

    public ConcurrentSender send(final TaggedLogAPIEntity entity) throws InterruptedException {
        if(!this.isStarted){
            this.start();
        }
        this.queue.put(entity);
        return this;
    }

    @Override
    public void close() throws IOException {
        for(Handler handler: handlers){
            handler.close();
        }
    }

    private class Handler extends BatchSender implements Runnable{
        private final long batchInterval;
        private final SynchronousQueue<TaggedLogAPIEntity> localQueue;

        private boolean isStopped;
        private long lastFlushTime;

        public Handler(SynchronousQueue<TaggedLogAPIEntity> queue, IEagleServiceClient client, int batchSize, long batchInterval) {
            super(client, batchSize);
            this.localQueue = queue;
            this.batchInterval = batchInterval;
        }

        @Override
        public void run() {
            if(LOG.isDebugEnabled()) LOG.debug("Starting ...");
            lastFlushTime = System.currentTimeMillis();

            while(!isStopped){
                TaggedLogAPIEntity entity = null;
                try {
                    entity = this.localQueue.take();
                } catch (InterruptedException e) {
                    LOG.error(e.getMessage(),e);
                }

                if(entity!=null){
                    try {
                        this.send(entity);
                    } catch (IOException e) {
                        LOG.error(e.getMessage(),e);
                    } catch (EagleServiceClientException e) {
                        LOG.error(e.getMessage(),e);
                    }
                    long currentTimestamp = System.currentTimeMillis();

                    if((currentTimestamp - this.lastFlushTime) >= this.batchInterval){
                        if(LOG.isDebugEnabled())
                            LOG.info(String.format("%s - %s >= %s",currentTimestamp,this.lastFlushTime,this.batchInterval));

                        try {
                            this.flush();
                        } catch (IOException e) {
                            LOG.error(e.getMessage(),e);
                        } catch (EagleServiceClientException e) {
                            LOG.error(e.getMessage(),e);
                        }
                    }
                }else{
                    LOG.warn("Got null entity");
                }
            }

            if(LOG.isDebugEnabled()) LOG.debug("Stopping ...");
        }

        @Override
        public void close() throws IOException {
            this.isStopped = true;
            super.close();
        }

        @Override
        public void flush() throws IOException, EagleServiceClientException {
            super.flush();
            this.lastFlushTime = System.currentTimeMillis();
        }
    }
}
