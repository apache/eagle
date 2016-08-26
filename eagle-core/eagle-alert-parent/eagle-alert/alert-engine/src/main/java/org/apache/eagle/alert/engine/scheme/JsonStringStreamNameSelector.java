/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *    http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.eagle.alert.engine.scheme;

import java.util.Map;
import java.util.Properties;

import org.apache.eagle.alert.coordination.model.StreamNameSelector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A strategy to get stream name from message tuple.
 * 
 * Since 5/5/16.
 */
public class JsonStringStreamNameSelector implements StreamNameSelector {
    private final static Logger LOG = LoggerFactory.getLogger(JsonStringStreamNameSelector.class);
    public final static String USER_PROVIDED_STREAM_NAME_PROPERTY = "userProvidedStreamName";
    public final static String FIELD_NAMES_TO_INFER_STREAM_NAME_PROPERTY = "fieldNamesToInferStreamName";
    public final static String STREAM_NAME_FORMAT = "streamNameFormat";

    private String userProvidedStreamName;
    private String[] fieldNamesToInferStreamName;
    private String streamNameFormat;

    public JsonStringStreamNameSelector(Properties prop) {
        userProvidedStreamName = prop.getProperty(USER_PROVIDED_STREAM_NAME_PROPERTY);
        String fields = prop.getProperty(FIELD_NAMES_TO_INFER_STREAM_NAME_PROPERTY);
        if (fields != null) {
            fieldNamesToInferStreamName = fields.split(",");
        }
        streamNameFormat = prop.getProperty(STREAM_NAME_FORMAT);
        if (streamNameFormat == null) {
            LOG.warn("no stream name format found, this might cause default stream name be used which is dis-encouraged. Possibly this is a mis-configuration.");
        }
    }

    @Override
    public String getStreamName(Map<String, Object> tuple) {
        if (userProvidedStreamName != null) {
            return userProvidedStreamName;
        } else if (fieldNamesToInferStreamName != null && streamNameFormat != null) {
            Object[] args = new Object[fieldNamesToInferStreamName.length];
            for (int i = 0; i < fieldNamesToInferStreamName.length; i++) {
                Object colValue = tuple.get(fieldNamesToInferStreamName[i]);
                args[i] = colValue;
            }
            return String.format(streamNameFormat, args);
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("can not find the stream name for data source. Use the default stream, possibly this means mis-configuration of datasource!");
        }
        return "defaultStringStream";
    }
    
}