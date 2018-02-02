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
package org.apache.eagle.alert.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang.StringUtils;
import org.codehaus.jettison.json.JSONArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class JsonUtils {

    public static final ObjectMapper mapper = new ObjectMapper();
    private static final Logger LOG = LoggerFactory.getLogger(JsonUtils.class);

    public static String writeValueAsString(Object o) {
        try {
            return mapper.writeValueAsString(o);
        } catch (Exception e) {
            LOG.error("write object as string failed {} !", o);
        }
        return "";
    }

    public static List<String> jsonStringToList(String message) {
        List<String> result = new ArrayList<>();
        try {
            if (!message.isEmpty()) {
                JSONArray jsonArray = new JSONArray(message);
                for (int i = 0; i < jsonArray.length(); ++i) {
                    result.add(jsonArray.getString(i));
                }
            }
        } catch (Exception e) {
            LOG.warn("illegal json array message: {}, ignored", StringUtils.abbreviate(message, 50));
        }

        return result;
    }
}
