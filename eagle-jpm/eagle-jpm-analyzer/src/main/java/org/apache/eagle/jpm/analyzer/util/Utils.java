/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

package org.apache.eagle.jpm.analyzer.util;

import com.typesafe.config.Config;
import org.apache.eagle.common.rest.RESTResponse;
import org.apache.eagle.jpm.analyzer.meta.model.JobMetaEntity;
import org.apache.eagle.jpm.analyzer.meta.model.UserEmailEntity;
import org.apache.eagle.jpm.util.resourcefetch.connection.InputStreamUtils;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.net.URLEncoder;
import java.util.*;

public class Utils {
    private static final Logger LOG = LoggerFactory.getLogger(Utils.class);

    private static final ObjectMapper OBJ_MAPPER = new ObjectMapper();

    static {
        OBJ_MAPPER.configure(JsonParser.Feature.ALLOW_NON_NUMERIC_NUMBERS, true);
    }

    public static List<JobMetaEntity> getJobMeta(Config config, String siteId, String jobDefId) {
        List<JobMetaEntity> result = new ArrayList<>();
        String url = "http://"
                + config.getString(Constants.HOST_PATH)
                + ":"
                + config.getInt(Constants.PORT_PATH)
                + config.getString(Constants.CONTEXT_PATH)
                + Constants.ANALYZER_PATH
                + Constants.JOB_META_ROOT_PATH
                + "/"
                + siteId
                + "/"
                + URLEncoder.encode(jobDefId);

        InputStream is = null;
        try {
            is = InputStreamUtils.getInputStream(url, null, org.apache.eagle.jpm.util.Constants.CompressionType.NONE);
            LOG.info("get job meta from {}", url);
            result = ((RESTResponse<List<JobMetaEntity>>)OBJ_MAPPER.readValue(is, new TypeReference<RESTResponse<List<JobMetaEntity>>>(){})).getData();
        } catch (Exception e) {
            LOG.warn("failed to get job meta from {}", url, e);
        } finally {
            org.apache.eagle.jpm.util.Utils.closeInputStream(is);
            return result;
        }
    }

    public static List<UserEmailEntity> getUserMail(Config config, String siteId, String userId) {
        List<UserEmailEntity> result = new ArrayList<>();
        String url = "http://"
                + config.getString(Constants.HOST_PATH)
                + ":"
                + config.getInt(Constants.PORT_PATH)
                + config.getString(Constants.CONTEXT_PATH)
                + Constants.ANALYZER_PATH
                + Constants.USER_META_ROOT_PATH
                + "/"
                + siteId
                + "/"
                + URLEncoder.encode(userId);

        InputStream is = null;
        try {
            is = InputStreamUtils.getInputStream(url, null, org.apache.eagle.jpm.util.Constants.CompressionType.NONE);
            LOG.info("get user meta from {}", url);
            result = ((RESTResponse<List<UserEmailEntity>>)OBJ_MAPPER.readValue(is, new TypeReference<RESTResponse<List<UserEmailEntity>>>(){})).getData();
        } catch (Exception e) {
            LOG.warn("failed to get user meta from {}", url, e);
        } finally {
            org.apache.eagle.jpm.util.Utils.closeInputStream(is);
            return result;
        }
    }

    public static <K, V extends Comparable<? super V>> List<Map.Entry<K, V>> sortByValue(Map<K, V> map) {
        List<Map.Entry<K, V>> list = new LinkedList<>(map.entrySet());
        Collections.sort(list, (e1, e2) -> e1.getValue().compareTo(e2.getValue()));
        Collections.reverse(list);
        return list;
    }
}
