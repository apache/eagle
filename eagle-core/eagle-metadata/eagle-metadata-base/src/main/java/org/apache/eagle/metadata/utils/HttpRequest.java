/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.eagle.metadata.utils;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class HttpRequest {
    private final static Logger LOG = LoggerFactory.getLogger(HttpRequest.class);

    private static JSONObject parseToJSON(CloseableHttpResponse response) throws IOException {
        JSONObject result;
        BufferedReader rd = new BufferedReader(new InputStreamReader(response.getEntity().getContent()));
        StringBuffer res = new StringBuffer();
        String line = "";
        while ((line = rd.readLine()) != null) {
            res.append(line);
        }
        result = new JSONObject(res.toString());
        return result;
    }

    public static JSONObject httpGetWithoutCredentials(String restUrl){
        HttpGet get = null;
        JSONObject result = null;
        CloseableHttpClient httpClient = null;

        try {
            get = new HttpGet(restUrl);
            result = new JSONObject();
            httpClient = HttpClients.createDefault();
            CloseableHttpResponse httpResponse =  httpClient.execute(get);
            result = parseToJSON(httpResponse);
            httpResponse.close();
        } catch (IOException e){
            LOG.info("Failed to parse httpResponse to JSONObject");
        } catch (Exception e){
            LOG.info("Can not execute GET request: ", e);
        }finally{
            try {
                if(httpClient !=null)
                    httpClient.close();
            } catch (IOException e){
                LOG.debug("httpClient can not be closed",e);
            }
            if(get!=null){
                get.releaseConnection();
            }
        }
        return result;
    }
}
