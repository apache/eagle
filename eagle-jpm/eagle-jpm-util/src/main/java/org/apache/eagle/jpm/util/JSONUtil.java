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

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

public class JSONUtil {

    public static String getString(JSONObject obj, String field) {
        if (obj.containsKey(field)) {
            return (String) obj.get(field);
        }
        return null;
    }

    public static Integer getInt(JSONObject obj, String field) {
        if (obj.containsKey(field)) {
            return ((Long) obj.get(field)).intValue();
        }
        return null;
    }

    public static Long getLong(JSONObject obj, String field) {
        if (obj.containsKey(field)) {
            return (Long) obj.get(field);
        }
        return null;
    }

    public static Boolean getBoolean(JSONObject obj, String field) {
        if (obj.containsKey(field)) {
            return (Boolean) obj.get(field);
        }
        return null;
    }

    public static JSONObject getJSONObject(JSONObject obj, String field) {
        if (obj.containsKey(field)) {
            return (JSONObject) obj.get(field);
        }
        return null;
    }

    public static JSONArray getJSONArray(JSONObject obj, String field) {
        if (obj.containsKey(field)) {
            return (JSONArray) obj.get(field);
        }
        return null;
    }
}
