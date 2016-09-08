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

import org.apache.eagle.alert.coordination.model.StreamNameSelector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;

/**
 * Since 5/3/16.
 */
public class PlainStringStreamNameSelector implements StreamNameSelector {
    @SuppressWarnings("unused")
    private static final Logger LOG = LoggerFactory.getLogger(PlainStringStreamNameSelector.class);
    private static final String USER_PROVIDED_STREAM_NAME_PROPERTY = "userProvidedStreamName";
    private static final String DEFAULT_STRING_STREAM_NAME = "defaultStringStream";

    private String streamName;

    public PlainStringStreamNameSelector(Properties prop) {
        streamName = prop.getProperty(USER_PROVIDED_STREAM_NAME_PROPERTY);
        if (streamName == null) {
            streamName = DEFAULT_STRING_STREAM_NAME;
        }
    }

    @Override
    public String getStreamName(Map<String, Object> tuple) {
        return streamName;
    }
}
