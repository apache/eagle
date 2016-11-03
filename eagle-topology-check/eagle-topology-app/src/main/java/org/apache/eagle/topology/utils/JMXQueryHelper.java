/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.eagle.topology.utils;

import org.apache.eagle.app.utils.connection.URLConnectionUtils;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.URLConnection;
import java.util.HashMap;
import java.util.Map;

/**
 * Helper class to query Hadoop JMX servlets.
 */
public final class JMXQueryHelper {

    private static final int DEFAULT_QUERY_TIMEOUT = 30 * 60 * 1000;
    private static final Logger LOG = LoggerFactory.getLogger(JMXQueryHelper.class);

    public static Map<String, JMXBean> query(String jmxQueryUrl) throws JSONException, IOException {
        LOG.info("Going to query JMX url: " + jmxQueryUrl);
        InputStream is = null;
        try {
            final URLConnection connection = URLConnectionUtils.getConnection(jmxQueryUrl);
            connection.setReadTimeout(DEFAULT_QUERY_TIMEOUT);
            is = connection.getInputStream();
            return parseStream(is);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        } finally {
            if (is != null) {
                is.close();
            }
        }
    }

    public static Map<String, JMXBean> parseStream(InputStream is) {
        final Map<String, JMXBean> resultMap = new HashMap<String, JMXBean>();
        final JSONTokener tokener = new JSONTokener(is);
        final JSONObject jsonBeansObject = new JSONObject(tokener);
        final JSONArray jsonArray = jsonBeansObject.getJSONArray("beans");
        int size = jsonArray.length();
        for (int i = 0; i < size; ++i) {
            final JSONObject obj = (JSONObject) jsonArray.get(i);
            final JMXBean bean = new JMXBean();
            final Map<String, Object> map = new HashMap<String, Object>();
            bean.setPropertyMap(map);
            final JSONArray names = obj.names();
            int jsonSize = names.length();
            for (int j = 0; j < jsonSize; ++j) {
                final String key = names.getString(j);
                Object value = obj.get(key);
                map.put(key, value);
            }
            final String nameString = (String) map.get("name");
            resultMap.put(nameString, bean);
        }
        return resultMap;
    }

}
