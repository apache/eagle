/*
 *
 *    Licensed to the Apache Software Foundation (ASF) under one or more
 *    contributor license agreements.  See the NOTICE file distributed with
 *    this work for additional information regarding copyright ownership.
 *    The ASF licenses this file to You under the Apache License, Version 2.0
 *    (the "License"); you may not use this file except in compliance with
 *    the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 *
 */
package org.apache.eagle.contrib.connector.webservice;

import org.apache.commons.codec.binary.Base64;
import org.apache.eagle.contrib.connector.policy.policyobject.PolicyBase;
import org.apache.eagle.contrib.connector.webservice.common.ServiceConstants;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.Charset;
import java.util.*;

public class EagleServiceProvider {
    public final static Logger LOG = LoggerFactory.getLogger(EagleServiceProvider.class);
    /**
     * IP address or hostname  of eagle service host
     */
    private String host = "localhost";
    /**
     * port number of eagle service
     */
    private String port = "9099";
    /**
     * username of eagle service
     */
    private String username = "admin";
    /**
     * password of of eagle service
     */
    private String password = "secret";


    public EagleServiceProvider(String host, String port, String username, String password) {
        this.host = host;
        this.port = port;
        this.username = username;
        this.password = password;
    }

    public String getHost() {
        return this.host;
    }

    public String getPort() {
        return this.port;
    }

    public String getUsername() {
        return this.username;
    }

    public String getPassword() {
        return this.password;
    }

    /**
     * create or update multiple Hdfs policies in Eagle
     *
     * @param policySet Policies collections
     * @return JSONObject, which is from a HTTP Response
     */
    public JSONObject createPolicyBatch(Set<PolicyBase> policySet) {
        JSONObject result = null;
        StringBuilder jsonData = new StringBuilder(800);
        if (0 == policySet.size()) return result;

        jsonData.append("[");
        Iterator<PolicyBase> iterator = policySet.iterator();
        while (iterator.hasNext()) {
            PolicyBase PolicyBase = iterator.next();
            jsonData.append(PolicyBase.toJSONData());
            if (iterator.hasNext()) jsonData.append(",");
        }
        jsonData.append("]");

        try {
            String restUrl = "http://" + host + ":" + port + ServiceConstants.URL_OF_RESTAPI + "?serviceName=" + ServiceConstants.ALERT_DEFINITION_SERVICE;
            result = executePost(restUrl, jsonData.toString());
        } catch (Exception e) {
            LOG.info(e.getMessage());
        }
        return result;
    }

    /**
     * Create/Update a  HDFS policy
     *
     * @param PolicyBase set a policyObject
     * @return Returns a JSONObject, which is a HTTP Response
     */
    public JSONObject createPolicy(PolicyBase PolicyBase) {
        JSONObject result = new JSONObject();
        Set<PolicyBase> set = new HashSet<>();
        set.add(PolicyBase);
        try {
            result = createPolicyBatch(set);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }

    /**
     * Get policy details
     *
     * @param policyQueryParams PolicyQueryParams
     * @return JSONObject , contains multiple policy definitions
     */
    public JSONObject getPolicy(PolicyQueryParams policyQueryParams, int pageSize){
        JSONObject jsonObject = new JSONObject();
        try {
            String restUrl = "http://" + host + ":" + port + ServiceConstants.URL_OF_RESTAPI + "?query=" + ServiceConstants.ALERT_DEFINITION_SERVICE + "[" + URLEncoder.encode(policyQueryParams.getParams(), "UTF-8") + "]";
            restUrl = restUrl + URLEncoder.encode("{*}", "UTF-8") + "&pageSize=" + pageSize;
            jsonObject = executeGet(restUrl);
        } catch (IOException e) {
            LOG.info(e.getMessage());
        }
        return jsonObject;

    }

    public JSONObject getPolicy(PolicyQueryParams policyQueryParams) throws UnsupportedEncodingException {
        return getPolicy(policyQueryParams, 100);
    }

    /**
     * Response parser, convert response to a json object
     *
     * @return JSONObject
     */
    JSONObject responseParser(HttpResponse httpResponse) throws IOException {
        BufferedReader rd = new BufferedReader(
                new InputStreamReader(httpResponse.getEntity().getContent()));

        StringBuffer result = new StringBuffer();
        String line;
        while ((line = rd.readLine()) != null) {
            result.append(line);
        }
        JSONObject jsonObject = new JSONObject(result.toString());
        return jsonObject;
    }

    /**
     * Delete  Eagle policy
     *
     * @param policyQueryParams PolicyQueryParams
     * @return JSONObject, http response
     */
    public JSONObject deletePolicy(PolicyQueryParams policyQueryParams) throws IOException {
        JSONObject result = new JSONObject();
        try {
            ArrayList<String> encodedRowkeys = getEncodedRowkeys(policyQueryParams);
            if (encodedRowkeys.size() == 0) return result;
            //crate url and post send
            String restUrl = "http://" + host + ":" + port + ServiceConstants.URL_OF_RESTAPI + "/delete?serviceName=" + ServiceConstants.ALERT_DEFINITION_SERVICE + "&byId=true";
            StringBuilder jsonData = new StringBuilder();
            jsonData.append("[");
            for (int i = 0; i < encodedRowkeys.size(); i++) {
                jsonData.append("\"" + encodedRowkeys.get(i) + "\"");
                if(i != encodedRowkeys.size()-1) jsonData.append(",");
            }
            jsonData.append("]");
            result = executePost(restUrl, jsonData.toString());
        } catch (Exception e) {
            LOG.info(e.getMessage());
        }
        return result;
    }

    /**
     * Get all encodedRowKey for selected policies
     *
     * @param policyQueryParams
     */
    public ArrayList<String> getEncodedRowkeys(PolicyQueryParams policyQueryParams) throws UnsupportedEncodingException {
        ArrayList<String> encodedRowkeys = new ArrayList<String>();
        JSONObject jsonObject;
        try {
            jsonObject = getPolicy(policyQueryParams);
            JSONArray jsonArray = (JSONArray) jsonObject.get("obj");
            for (int i = 0; i < jsonArray.length(); i++) {
                JSONObject data = (JSONObject) jsonArray.get(i);
                encodedRowkeys.add(data.getString("encodedRowkey"));
            }
        } catch (Exception e) {
            LOG.info(e.getMessage());
        }
        return encodedRowkeys;
    }

    /**
     * mark folder/file with sensitivity type
     *
     * @param site            Name of site
     * @param fileSensitivity key is a full filePath, value is a set which contains all file sensitivity type
     */
    public JSONObject setSensitivity(String site, Map<String, Set<String>> fileSensitivity) {
        JSONObject result = new JSONObject();
        String restUrl = "http://" + host + ":" + port + ServiceConstants.URL_OF_RESTAPI + "?serviceName=" + ServiceConstants.FILE_SENSITIVITY_SERVICE;

        //construct json data
        StringBuilder jsonData = new StringBuilder(100);
        jsonData.append("[");
        Iterator iterator = fileSensitivity.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry pair = (Map.Entry) iterator.next();
            //construct a json data
            String fPath = (String) pair.getKey();
            Set<String> fType = (Set<String>) pair.getValue();
            String tag = fTypeToTag(fType);
            jsonData.append("{\"tags\":{\"site\" : \"" + site + "\",\"filedir\" : \"" + fPath + "\"},\"sensitivityType\": \"" + tag + "\"}");
            if (iterator.hasNext()) jsonData.append(",");
        }
        jsonData.append("]");

        try {
            result = executePost(restUrl, jsonData.toString());
        } catch (Exception e) {
            LOG.info(e.getMessage());
        }
        return result;
    }

    /**
     * set single file with its sensitivity
     *
     * @param site   siteName
     * @param fPath  Full path of the file/folder
     * @param fTypes contains all sensitivity types of this file/folder
     */
    public JSONObject setSensitivity(String site, String fPath, Set<String> fTypes) throws Exception {
        JSONObject result;
        Map<String, Set<String>> map = new HashMap<>();
        map.put(fPath, fTypes);
        result = setSensitivity(site, map);
        return result;
    }

    /**
     * @return concatenate the types in the form: "type1|type2|type3"
     * @params fTypes contains all the sensitive types
     */
    private String fTypeToTag(Set<String> fTypes) {
        StringBuilder tag = new StringBuilder();
        Iterator iterator = fTypes.iterator();
        if (iterator.hasNext()) {
            tag.append(iterator.next());
        }
        while (iterator.hasNext()) {
            tag.append('|').append(iterator.next());
        }
        return tag.toString();
    }


    /**
     * delete folder/file with sensitivity type
     *
     * @param site  site name
     * @param fPath full file/folder path
     */

    //remove  file's tag
    public JSONObject removeSensitivity(String site, String fPath) throws Exception {
        JSONObject result;
        Set<String> set = new HashSet<>();
        set.add(fPath);
        result = removeSensitivity(site, set);
        return result;
    }

    // remove multiple files' tag
    public JSONObject removeSensitivity(String site, Set<String> fPaths) throws Exception {
        JSONObject result = new JSONObject();
        Map<String, Set<String>> map = new HashMap<>();
        HashSet<String> empty = new HashSet<>();
        for (String fPath : fPaths) {
            map.put(fPath, empty);
        }
        result = setSensitivity(site, map);
        return result;
    }

    /**
     * Get sensitivity of a folder/file
     *
     * @param site  site name
     * @param fpath full path of file/folder, eg: /folder
     */
    public String getSensitivityTag(String site, String fpath) throws UnsupportedEncodingException {
        String tag = null;
        String restUrl = "http://" + host + ":" + port + ServiceConstants.URL_OF_RESTAPI + "?query=" + ServiceConstants.FILE_SENSITIVITY_SERVICE
                + "["
                + URLEncoder.encode("@site=" + "\"" + site + "\"" + " and @filedir=" + "\"" + fpath + "\"", "UTF-8")
                + "]";
        restUrl = restUrl + URLEncoder.encode("{*}", "UTF-8") + "&pageSize=100";
        try {
            JSONObject jsonObject = executeGet(restUrl);
            JSONArray jsonArray = jsonObject.getJSONArray("obj");
            JSONObject fData = jsonArray.getJSONObject(0);
            tag = fData.getString("sensitivityType");
        } catch (Exception e) {
            LOG.info(e.getMessage());
        }
        return tag;
    }


    /**
     * Get sensitivity meta data of a site
     *
     * @param site site name
     */
    public Map<String, String> getAllSensitivityTag(String site) throws UnsupportedEncodingException {
        Map<String, String> map = new HashMap<String, String>();
        String restUrl = "http://" + host + ":" + port + ServiceConstants.URL_OF_RESTAPI + "?query=" + ServiceConstants.FILE_SENSITIVITY_SERVICE + URLEncoder.encode("[@site=" + "\"" + site + "\"" + "]", "UTF-8");
        restUrl = restUrl + URLEncoder.encode("{*}", "UTF-8") + "&pageSize=100";
        try {
            JSONObject jsonObject = executeGet(restUrl);
            JSONArray jsonArray = jsonObject.getJSONArray("obj");
            for (int i = 0; i < jsonArray.length(); i++) {
                JSONObject fdata = jsonArray.getJSONObject(i);
                String filedir = fdata.getJSONObject("tags").getString("filedir");
                String fType = fdata.getString("sensitivityType");
                map.put(filedir, fType);
            }
        } catch (Exception e) {
            LOG.info(e.getMessage());
        }
        return map;
    }


    /**
     * This method is used to create HttpPost with authentication header
     *
     * @param restUrl restURl of Eagle Service
     * @return return HttpPost request based on authentication
     * *
     */
     HttpPost createPost(String restUrl, String jsonData) throws UnsupportedEncodingException {
        HttpPost post;
        post = new HttpPost(restUrl);
        String auth = new StringBuffer(username).append(":").append(password).toString();
        byte[] encodedAuth = Base64.encodeBase64(auth.getBytes(Charset.forName("US-ASCII")));
        String authHeader = "Basic " + new String(encodedAuth);
        post.setHeader("AUTHORIZATION", authHeader);
        post.setHeader("Content-Type", "application/json");
        post.setEntity(new StringEntity(jsonData));
        return post;
    }

    /**
     * This method is used to create HttpGet with authentication header
     *
     * @param restUrl restURl of Eagle Service
     * @return return HttpGet request based on authentication
     * *
     */
     HttpGet createGet(String restUrl) {
        HttpGet get;
        get = new HttpGet(restUrl);
        String auth = new StringBuffer(username).append(":").append(password).toString();
        byte[] encodedAuth = Base64.encodeBase64(auth.getBytes(Charset.forName("US-ASCII")));
        String authHeader = "Basic " + new String(encodedAuth);
        get.setHeader("AUTHORIZATION", authHeader);
        get.setHeader("Content-Type", "application/json");
        return get;
    }

    /**
     * @return JSONObject. Execute a http GET and return a JSONObject based on http response
     * */

    JSONObject executeGet(String restUrl){
        HttpGet httpGet = createGet(restUrl);
        JSONObject result = new JSONObject();
        HttpClient httpClient = HttpClientBuilder.create().build();
        try {
            HttpResponse httpResponse = httpClient.execute(httpGet);
            result = responseParser(httpResponse);
        }catch (Exception e){
            LOG.info(e.getMessage());
        }
        httpGet.releaseConnection();
        return result;
    }

    /**
     * @return JSONObject. Execute a http POST and return a JSONObject based on http response
     * */
    JSONObject executePost(String restUrl, String data){
        JSONObject result = new JSONObject();
        try {
            HttpPost httpPost = createPost(restUrl, data);
            HttpClient httpClient = HttpClientBuilder.create().build();
            HttpResponse response = httpClient.execute(httpPost);
            result = responseParser(response);
            httpPost.releaseConnection();
        }catch (Exception e){
            LOG.info(e.getMessage());
        }
        return result;
    }

}

