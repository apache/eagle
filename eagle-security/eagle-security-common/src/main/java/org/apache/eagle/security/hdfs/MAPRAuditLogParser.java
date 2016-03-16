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
package org.apache.eagle.security.hdfs;

import org.apache.eagle.common.DateTimeUtil;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;


public final class MAPRAuditLogParser {
    private final static Logger LOG = LoggerFactory.getLogger(MAPRAuditLogParser.class);

    public MAPRAuditLogParser(){
    }

    public HDFSAuditLogObject maprParser(String log) throws JSONException, ParseException {
        JSONObject jsonObject = new JSONObject(log);
        String timestamp = jsonObject.getJSONObject("timestamp").getString("$date");
        String cmd = jsonObject.getString("operation");
        String user = jsonObject.getString("uid");
        String ip = jsonObject.getString("ipAddress");
        String src;
        String dst;
        try{
            src = jsonObject.getString("srcFid");
        } catch (JSONException e){
            src = "null";
        }
        try{
            dst = jsonObject.getString("dstFid");
        }catch (JSONException e){
            dst = "null";
        }

        String status = String.valueOf(jsonObject.getString("status"));
        Boolean allowed = false;
        if(status.equals("0"))  allowed = true;

        HDFSAuditLogObject entity = new HDFSAuditLogObject();
        entity.user = user;
        entity.cmd = cmd;
        entity.src = src;
        entity.dst = dst;
        entity.host = ip;
        entity.allowed =allowed;
        entity.timestamp = DateTimeUtil.maprhumanDateToMilliseconds(timestamp);
        return entity;
    }

}
