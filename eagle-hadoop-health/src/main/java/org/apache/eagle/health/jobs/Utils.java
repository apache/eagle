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
package org.apache.eagle.health.jobs;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class Utils {
    private static final Logger LOG = LoggerFactory.getLogger(Utils.class);

    public static Integer getInt(Properties properties, String key, int defaultValue) {
        Object obj = properties.get(key);
        if (obj != null) {
            try {
                if (obj instanceof Integer) {
                    return (Integer) obj;
                } else {
                    return Integer.parseInt(obj.toString());
                }
            } catch (Exception ex) {
                throw new IllegalArgumentException("Failed to read value: " + obj + " as integer for property: " + key + ", due to: " + ex.getMessage(), ex);
            }
        } else {
            return defaultValue;
        }
    }

    public static boolean containsKey(Properties properties, String key) {
        return properties.get(key) != null;
    }

    public static void merge(Properties from, Configuration to) {
        for (String propertyName : from.stringPropertyNames()) {
            Object propertiesValue = from.get(propertyName);
            if (propertiesValue instanceof String) {
                to.set(propertyName, (String) propertiesValue);
            } else if ((propertiesValue instanceof Integer)) {
                to.setInt(propertyName, (Integer) propertiesValue);
            } else if ((propertiesValue instanceof Long)) {
                to.setLong(propertyName, (Long) propertiesValue);
            } else if ((propertiesValue instanceof Float)) {
                to.setFloat(propertyName, (Float) propertiesValue);
            } else if ((propertiesValue instanceof Boolean)) {
                to.setBoolean(propertyName, (Boolean) propertiesValue);
            } else {
                LOG.warn("Skip merging " + propertyName + ":" + propertiesValue);
            }
        }
    }

    public static String getString(Properties properties, String key, String defaultValue) {
        Object obj = properties.get(key);
        if (obj != null && obj instanceof String) {
            return (String) obj;
        } else {
            return defaultValue;
        }
    }

    private static final int SIZEOF_INT = Integer.SIZE / Byte.SIZE;

    /**
     * Converts a byte array to an int value.
     *
     * @param bytes byte array
     * @return the int value
     * @throws IllegalArgumentException if length is not {@link #SIZEOF_INT}
     */
    public static int toInt(byte[] bytes) {
        if (SIZEOF_INT > bytes.length) {
            throw new IllegalArgumentException("length is not SIZEOF_INT");
        }

        int n = 0;
        for (int i = 0; i < +bytes.length; i++) {
            n <<= 8;
            n ^= bytes[i] & 0xFF;
        }
        return n;
    }
}