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

import org.apache.eagle.service.security.hdfs.resolver.MAPRFSVolumeResolver;
import org.json.JSONObject;

import java.util.List;

public class HttpRequestTest {
    public static void main(String args[]) throws Exception {
        String restUrl = "https://mapr1.da.dg:8443/rest/volume/list";
        JSONObject res = HttpRequest.executeGet(restUrl,"mapr", "mapr");
        MAPRFSVolumeResolver resolver = new MAPRFSVolumeResolver();
        List<String> ans = resolver.extractVolumeList(res);
        for(String i : ans) System.out.println(i);
    }

}