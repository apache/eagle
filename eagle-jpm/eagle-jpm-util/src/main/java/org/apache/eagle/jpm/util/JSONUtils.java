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

package org.apache.eagle.jpm.util;

import org.apache.commons.lang.StringUtils;
import org.json.JSONException;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

public class JSONUtils {

    public static String getString(JSONObject obj, String field) {
        if (obj == null || StringUtils.isEmpty(field)) {
            return null;
        }

        try {
            return obj.get(field).toString();
        } catch (JSONException e) {
            e.printStackTrace();
            return null;
        }
    }

    public static int getInt(JSONObject obj, String field) {
        if (obj == null || StringUtils.isEmpty(field)) {
            return 0;
        }

        try {
            return (int) obj.get(field);
        } catch (JSONException e) {
            e.printStackTrace();
            return 0;
        }
    }

    public static long getLong(JSONObject obj, String field) {
        return getLong(obj, field, 0L);
    }

    public static long getLong(JSONObject obj, String field, long defaultValue) {
        if (obj == null || StringUtils.isEmpty(field)) {
            return defaultValue;
        }

        try {
            return (long) obj.get(field);
        } catch (JSONException e) {
            e.printStackTrace();
            return defaultValue;
        }
    }

    public static Boolean getBoolean(JSONObject obj, String field) {
        if (obj == null || StringUtils.isEmpty(field)) {
            return false;
        }

        try {
            return (Boolean) obj.get(field);
        } catch (JSONException e) {
            e.printStackTrace();
            return false;
        }
    }

    public static JSONObject getJSONObject(JSONObject obj, String field) {
        if (obj == null || StringUtils.isEmpty(field)) {
            return null;
        }

        try {
            return (JSONObject) obj.get(field);
        } catch (JSONException e) {
            e.printStackTrace();
            return null;
        }
    }

    public static JSONArray getJSONArray(JSONObject obj, String field) {
        if (obj == null || StringUtils.isEmpty(field)) {
            return null;
        }

        try {
            return (JSONArray) obj.get(field);
        } catch (JSONException e) {
            e.printStackTrace();
            return null;
        }
    }
}
