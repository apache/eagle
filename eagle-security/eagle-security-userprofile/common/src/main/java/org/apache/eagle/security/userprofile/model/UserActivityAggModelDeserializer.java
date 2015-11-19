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
package org.apache.eagle.security.userprofile.model;

import org.apache.eagle.dataproc.impl.storm.kafka.SpoutKafkaMessageDeserializer;
import org.apache.eagle.log.base.taggedlog.TaggedLogAPIEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Properties;

public class UserActivityAggModelDeserializer implements SpoutKafkaMessageDeserializer {
    private static Logger LOG = LoggerFactory.getLogger(UserActivityAggModelDeserializer.class);
    private final Properties props;
    private final static String CHARSET = "UTF-8";

    public  UserActivityAggModelDeserializer(Properties props){
        this.props = props;
    }

    @Override
    public Object deserialize(byte[] bytes) {
        String str = null;
        try {
            str = new String(bytes,CHARSET);
            return TaggedLogAPIEntity.buildObjectMapper().readValue(str, UserActivityAggModelEntity.class);
        } catch (UnsupportedEncodingException e) {
            LOG.error("Not support encoding: " + CHARSET, e);
            throw new IllegalStateException("Not support "+CHARSET,e);
        } catch (IOException e) {
            LOG.warn("Failed to deserialize as UserActivityAggModel, ignored: "+str,e);
//            throw new IllegalArgumentException("Failed to deserialize: "+str,e);
        }
        return null;
    }
}