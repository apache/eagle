/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.eagle.topology;

import org.apache.eagle.topology.entity.JournalNodeServiceAPIEntity;
import org.apache.eagle.topology.utils.EntityBuilderHelper;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TestJsonParser {

    @Test
    public void test1() {
        String jnInfoString = "{\"LastAppliedOrWrittenTxId\":\"71349818863\",\"MostRecentCheckpointTxId\":\"71337509096\"}";
        JSONObject jsonObject = new JSONObject(jnInfoString);
        String lastTxId = jsonObject.getString("LastAppliedOrWrittenTxId");
        Assert.assertTrue(lastTxId.equals("71349818863"));
    }

    @Test
    public void test2() throws UnknownHostException {
        String journalnodeString = "[{\"stream\":\"Writing segment beginning at txid 71349604348. \\n192.168.201.0:8485 (Written txid 71349818862), 192.168.201.1:8485 (Written txid 71349818862), 192.168.201.2:8485 (Written txid 71349818862), 192.168.201.3:8485 (Written txid 71349818862), 192.168.201.4:8485 (Written txid 71349818862)\",\"manager\":\"QJM to [192.168.201.0:8485, 192.168.201.1:8485, 192.168.201.3:8485, 192.168.201.4:8485, 192.168.201.5:8485]\",\"required\":\"true\",\"disabled\":\"false\"},{\"stream\":\"EditLogFileOutputStream(/hadoop/nn1/1/current/edits_inprogress_0000000071349604348)\",\"manager\":\"FileJournalManager(root=/hadoop/nn1/1)\",\"required\":\"false\",\"disabled\":\"false\"}]";
        JSONArray jsonArray = new JSONArray(journalnodeString);
        Assert.assertTrue(jsonArray.length() == 2);

        JSONObject jsonMap = (JSONObject) jsonArray.get(0);
        String QJM = jsonMap.getString("manager");
        Assert.assertTrue(QJM.equals("QJM to [192.168.201.0:8485, 192.168.201.1:8485, 192.168.201.3:8485, 192.168.201.4:8485, 192.168.201.5:8485]"));

        String STATUS_pattern = "([\\d\\.]+):\\d+\\s+\\([\\D]+(\\d+)\\)";
        String QJM_pattern = "([\\d\\.]+):\\d+";

        String stream = jsonMap.getString("stream");

        Pattern status = Pattern.compile(STATUS_pattern);
        Matcher statusMatcher = status.matcher(stream);
        List<JournalNodeServiceAPIEntity> entities = new ArrayList<>();
        while (statusMatcher.find()) {
            JournalNodeServiceAPIEntity entity = new JournalNodeServiceAPIEntity();
            entity.setTags(new HashMap<>());
            entity.getTags().put(TopologyConstants.HOSTNAME_TAG, statusMatcher.group(1));
            entity.setWrittenTxidDeviation(Long.parseLong(statusMatcher.group(2)));
            entities.add(entity);
        }
        Assert.assertTrue(entities.size() == 5);

        Pattern qjm = Pattern.compile(QJM_pattern);
        Matcher jpmMatcher = qjm.matcher(QJM);
        List<String> hosts = new ArrayList<>();
        while (jpmMatcher.find()) {
            hosts.add(EntityBuilderHelper.resolveHostByIp(jpmMatcher.group(1)));
        }

    }
}
