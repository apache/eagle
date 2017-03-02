/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.eagle.hadoop.jmx;

import org.apache.eagle.app.utils.connection.ServiceNotResponseException;
import org.apache.eagle.app.utils.connection.URLResourceFetcher;
import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONTokener;

import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

/**
 * Helper class to query Hadoop JMX servlets.
 */
public final class JMXQueryHelper {

    public static Map<String, JMXBean> query(String jmxQueryUrl) throws ServiceNotResponseException {
        InputStream is = null;
        try {
            is = URLResourceFetcher.openURLStream(jmxQueryUrl);
            return parseStream(is);
        } catch (Exception e) {
            throw new ServiceNotResponseException(e);
        } finally {
            URLResourceFetcher.closeInputStream(is);
        }
    }

    private static Map<String, JMXBean> parseStream(InputStream is) {
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
