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

package org.apache.eagle.jpm.util.resourceFetch;

import org.apache.eagle.jpm.util.Constants;
import org.apache.eagle.jpm.util.resourceFetch.connection.InputStreamUtils;
import org.apache.eagle.jpm.util.resourceFetch.model.SparkApplication;
import org.apache.eagle.jpm.util.resourceFetch.url.ServiceURLBuilder;
import org.apache.eagle.jpm.util.resourceFetch.url.SparkJobServiceURLBuilderImpl;
import org.apache.commons.codec.binary.Base64;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;

public class SparkHistoryServerResourceFetcher implements ResourceFetcher<SparkApplication> {

    private static final Logger LOG = LoggerFactory.getLogger(SparkHistoryServerResourceFetcher.class);

    private static final ObjectMapper OBJ_MAPPER = new ObjectMapper();

    private String historyServerURL;
    private final ServiceURLBuilder sparkDetailJobServiceURLBuilder;
    private String auth;

    static {
        OBJ_MAPPER.configure(JsonParser.Feature.ALLOW_NON_NUMERIC_NUMBERS, true);
    }

    public SparkHistoryServerResourceFetcher(String historyServerURL, String userName, String pwd){
        this.historyServerURL = historyServerURL;
        this.sparkDetailJobServiceURLBuilder = new SparkJobServiceURLBuilderImpl();
        this.auth = "Basic " + new String(new Base64().encode(String.format("%s:%s", userName, pwd).getBytes()));;
    }

    private List<SparkApplication> doFetchSparkApplicationDetail(String appId) throws Exception {
        InputStream is = null;
        try {
            final String urlString = sparkDetailJobServiceURLBuilder.build(this.historyServerURL, appId);
            LOG.info("Going to call spark history server api to fetch spark job: " + urlString);
            is = InputStreamUtils.getInputStream(urlString, auth, Constants.CompressionType.NONE);
            SparkApplication app = OBJ_MAPPER.readValue(is, SparkApplication.class);
            return Arrays.asList(app);
        } catch (FileNotFoundException e) {
            return null;
        } finally {
            if (is != null) { try {is.close();} catch (Exception e) { } }
        }
    }

    public List<SparkApplication> getResource(Constants.ResourceType resoureType, Object... parameter) throws Exception{
        switch(resoureType) {
            case SPARK_JOB_DETAIL:
                return doFetchSparkApplicationDetail((String)parameter[0]);
            default:
                throw new Exception("Not support resourceType :" + resoureType);
        }
    }
}
