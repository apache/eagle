/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.eagle.app.proxy.stream.impl;

import com.google.common.base.Preconditions;
import org.apache.eagle.alert.utils.StreamValidationException;
import org.apache.eagle.alert.utils.StreamValidator;
import org.apache.eagle.app.messaging.StreamRecord;
import org.apache.eagle.app.proxy.stream.StreamProxy;
import org.apache.eagle.app.proxy.stream.StreamProxyProducer;
import org.apache.eagle.metadata.model.StreamDesc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

public class StreamProxyImpl implements StreamProxy {
    private static final Logger LOGGER = LoggerFactory.getLogger(StreamProxyImpl.class);
    private StreamProxyProducer proxyProducer;
    private volatile boolean opened;
    private String streamId;
    private StreamValidator validator;

    @Override
    public void open(StreamDesc streamDesc) {
        if (streamDesc.getSchema() != null && streamDesc.getSchema().isValidate()) {
            this.validator = new StreamValidator(streamDesc.getSchema());
        }
        this.streamId = streamDesc.getStreamId();
        if (streamDesc.getSinkConfig() != null) {
            this.proxyProducer = new KafkaStreamProxyProducerImpl(streamDesc.getStreamId(), streamDesc.getSinkConfig());
        } else {
            LOGGER.warn("Unable to initialize kafka producer because sink config is null for {}", streamId);
            this.proxyProducer = null;
        }
        this.opened = true;
    }

    @Override
    public void close() throws IOException {
        if (this.proxyProducer != null) {
            this.proxyProducer.close();
        }
        this.opened = false;
    }

    @Override
    public void send(List<StreamRecord> events) throws IOException {
        Preconditions.checkArgument(this.opened, "Stream proxy not opened for " + streamId);
        Preconditions.checkNotNull(this.proxyProducer, "Stream proxy producer not initialized for " + streamId);
        if (this.validator != null) {
            for (StreamRecord event : events) {
                try {
                    this.validator.validateMap(event);
                } catch (StreamValidationException e) {
                    throw new IOException(e);
                }
            }
        }
        this.proxyProducer.send(events);
    }
}