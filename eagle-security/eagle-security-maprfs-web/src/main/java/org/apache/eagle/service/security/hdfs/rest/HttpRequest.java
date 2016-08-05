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
package org.apache.eagle.service.security.hdfs.rest;

import org.apache.commons.codec.binary.Base64;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;


public class HttpRequest {
    private static Logger LOG = LoggerFactory.getLogger(HttpRequest.class);
    /**
     * This method is used to create HttpGet with authentication header
     * @param restUrl restURl of web service
     * @return return HttpGet request based on authentication
     * * */
    public static JSONObject executeGet(String restUrl, String username, String password) throws Exception {
        HttpGet get;
        JSONObject result = new JSONObject();
        CloseableHttpResponse response = null;
        try {
            CloseableHttpClient httpclient = HttpClients.createDefault();

            HttpPost post = new HttpPost(restUrl);
            String auth=new StringBuffer(username).append(":").append(password).toString();
            byte[] encodedAuth = Base64.encodeBase64(auth.getBytes(Charset.forName("US-ASCII")));
            String authHeader = "Basic " + new String(encodedAuth);
            post.addHeader("Authorization", authHeader);
            post.addHeader("Content-Type", "application/json");
            response  = httpclient.execute(post);

            BufferedReader rd = new BufferedReader(
                    new InputStreamReader(response.getEntity().getContent()));

            StringBuffer res = new StringBuffer();
            String line = "";
            while ((line = rd.readLine()) != null) {
                res.append(line);
            }
            result = new JSONObject(res.toString());

        } catch (IOException e) {
            LOG.debug("Failed to execute http get request ", e);
        } finally {
            if(response !=null)response.close();
        }
        return result;
    }

}
