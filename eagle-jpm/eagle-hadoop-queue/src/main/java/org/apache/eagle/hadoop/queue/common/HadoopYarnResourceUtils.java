/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package org.apache.eagle.hadoop.queue.common;

import org.apache.eagle.jpm.util.Constants;
import org.apache.eagle.jpm.util.resourcefetch.connection.InputStreamUtils;
import com.typesafe.config.Config;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.InputStream;

public class HadoopYarnResourceUtils {

    private static final ObjectMapper OBJ_MAPPER = new ObjectMapper();

    static {
        OBJ_MAPPER.configure(JsonParser.Feature.ALLOW_NON_NUMERIC_NUMBERS, true);
    }

    public static Object getObjectFromUrlStream(String urlString, Class<?> clazz) throws Exception {
        InputStream is = null;
        Object o = null;
        try {
            is = readStream(urlString);
            o = OBJ_MAPPER.readValue(is, clazz);
        } catch (Exception e) {
            throw new IllegalArgumentException(String.format("Fetch resource %s failed", urlString), e);
        } finally {
            if (is != null) {
                is.close();
            }
        }
        return o;
    }

    private static InputStream readStream(String urlString) throws Exception {
        try {
            return InputStreamUtils.getInputStream(urlString, null, Constants.CompressionType.GZIP);
        } catch (java.util.zip.ZipException ex) {
            return InputStreamUtils.getInputStream(urlString, null, Constants.CompressionType.NONE);
        }
    }

    public static String getConfigValue(Config eagleConf, String key, String defaultValue) {
        if (eagleConf.hasPath(key)) {
            return eagleConf.getString(key);
        } else {
            return defaultValue;
        }
    }
}
