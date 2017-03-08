/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.eagle.security.traffic;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.eagle.common.DateTimeUtil;
import org.apache.eagle.security.auditlog.TopWindowResult;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.TimeZone;

public class TopWindowResultTest {

    @Test
    public void test() {
        String data = "{\"timestamp\":\"2017-03-07T21:36:51-0700\",\"windows\":[{\"ops\":[{\"opType\":\"rename (options=[NONE])\",\"topUsers\":[{\"user\":\"hadoop/apollo-phx-jt.vip.ebay.com@APD.EBAY.COM\",\"count\":24}],\"totalCount\":24},{\"opType\":\"rename (options=[OVERWRITE])\",\"topUsers\":[{\"user\":\"b_um@CORP.EBAY.COM\",\"count\":2}],\"totalCount\":2},{\"opType\":\"listStatus\",\"topUsers\":[{\"user\":\"b_bis@CORP.EBAY.COM\",\"count\":50061},{\"user\":\"hadoop/phxaishdc9en0014-be.phx.ebay.com@APD.EBAY.COM\",\"count\":12645},{\"user\":\"b_sd_research@CORP.EBAY.COM\",\"count\":5324},{\"user\":\"b_um\",\"count\":3584},{\"user\":\"b_kylin@CORP.EBAY.COM\",\"count\":2444},{\"user\":\"b_sscience@CORP.EBAY.COM\",\"count\":1999},{\"user\":\"b_des@APD.EBAY.COM\",\"count\":848},{\"user\":\"b_um@CORP.EBAY.COM\",\"count\":432},{\"user\":\"b_merch@CORP.EBAY.COM\",\"count\":413},{\"user\":\"b_eagle@APD.EBAY.COM\",\"count\":270}],\"totalCount\":90811},{\"opType\":\"setAcl\",\"topUsers\":[{\"user\":\"b_des@CORP.EBAY.COM\",\"count\":57},{\"user\":\"b_ebayadvertising@CORP.EBAY.COM\",\"count\":10}],\"totalCount\":67},{\"opType\":\"delete\",\"topUsers\":[{\"user\":\"hadoop/phxaishdc9en0014-be.phx.ebay.com@APD.EBAY.COM\",\"count\":761},{\"user\":\"b_des@CORP.EBAY.COM\",\"count\":36},{\"user\":\"b_ebayadvertising@CORP.EBAY.COM\",\"count\":24},{\"user\":\"appmon@APD.EBAY.COM\",\"count\":17},{\"user\":\"b_um@CORP.EBAY.COM\",\"count\":15},{\"user\":\"b_bis@CORP.EBAY.COM\",\"count\":14},{\"user\":\"hadoop/apollo-phx-jt.vip.ebay.com@APD.EBAY.COM\",\"count\":12},{\"user\":\"b_sd_research@CORP.EBAY.COM\",\"count\":9},{\"user\":\"b_merch@CORP.EBAY.COM\",\"count\":6},{\"user\":\"b_sscience@CORP.EBAY.COM\",\"count\":6}],\"totalCount\":917},{\"opType\":\"create\",\"topUsers\":[{\"user\":\"b_bis@CORP.EBAY.COM\",\"count\":1733},{\"user\":\"jdidis@CORP.EBAY.COM\",\"count\":1467},{\"user\":\"b_sd_research@CORP.EBAY.COM\",\"count\":1190},{\"user\":\"jguha@CORP.EBAY.COM\",\"count\":1106},{\"user\":\"b_slng\",\"count\":1103},{\"user\":\"b_merch@CORP.EBAY.COM\",\"count\":1095},{\"user\":\"b_kylin@CORP.EBAY.COM\",\"count\":373},{\"user\":\"b_des@CORP.EBAY.COM\",\"count\":344},{\"user\":\"b_find\",\"count\":133},{\"user\":\"appmon@APD.EBAY.COM\",\"count\":111}],\"totalCount\":9328},{\"opType\":\"setPermission\",\"topUsers\":[{\"user\":\"b_queryservice@CORP.EBAY.COM\",\"count\":76},{\"user\":\"b_des@CORP.EBAY.COM\",\"count\":48},{\"user\":\"b_um@CORP.EBAY.COM\",\"count\":18},{\"user\":\"b_bis@CORP.EBAY.COM\",\"count\":17},{\"user\":\"jguha@CORP.EBAY.COM\",\"count\":9},{\"user\":\"b_kylin@CORP.EBAY.COM\",\"count\":9},{\"user\":\"b_sitespeed@CORP.EBAY.COM\",\"count\":8},{\"user\":\"b_pandaren_kwdm@APD.EBAY.COM\",\"count\":8},{\"user\":\"b_merch@CORP.EBAY.COM\",\"count\":5},{\"user\":\"b_sd_research@CORP.EBAY.COM\",\"count\":3}],\"totalCount\":206},{\"opType\":\"contentSummary\",\"topUsers\":[{\"user\":\"b_bis@CORP.EBAY.COM\",\"count\":2},{\"user\":\"jguha@CORP.EBAY.COM\",\"count\":1}],\"totalCount\":3},{\"opType\":\"getEZForPath\",\"topUsers\":[{\"user\":\"b_des@CORP.EBAY.COM\",\"count\":102},{\"user\":\"b_ebayadvertising@CORP.EBAY.COM\",\"count\":5}],\"totalCount\":107},{\"opType\":\"getAclStatus\",\"topUsers\":[{\"user\":\"b_des@CORP.EBAY.COM\",\"count\":53},{\"user\":\"b_ebayadvertising@CORP.EBAY.COM\",\"count\":5}],\"totalCount\":58},{\"opType\":\"*\",\"topUsers\":[{\"user\":\"b_bis@CORP.EBAY.COM\",\"count\":267772},{\"user\":\"b_slng\",\"count\":94742},{\"user\":\"b_um@CORP.EBAY.COM\",\"count\":79454},{\"user\":\"b_merch@CORP.EBAY.COM\",\"count\":35677},{\"user\":\"b_sd_research@CORP.EBAY.COM\",\"count\":31074},{\"user\":\"b_sitespeed@CORP.EBAY.COM\",\"count\":17148},{\"user\":\"jdidis@CORP.EBAY.COM\",\"count\":15411},{\"user\":\"hadoop/phxaishdc9en0014-be.phx.ebay.com@APD.EBAY.COM\",\"count\":15185},{\"user\":\"b_kylin@CORP.EBAY.COM\",\"count\":7295},{\"user\":\"b_ebayadvertising@CORP.EBAY.COM\",\"count\":5806}],\"totalCount\":630619},{\"opType\":\"setReplication\",\"topUsers\":[{\"user\":\"b_queryservice@CORP.EBAY.COM\",\"count\":75},{\"user\":\"b_des@CORP.EBAY.COM\",\"count\":47},{\"user\":\"b_bis@CORP.EBAY.COM\",\"count\":18},{\"user\":\"jguha@CORP.EBAY.COM\",\"count\":4},{\"user\":\"b_kylin@CORP.EBAY.COM\",\"count\":3},{\"user\":\"b_um@CORP.EBAY.COM\",\"count\":3},{\"user\":\"b_sitespeed@CORP.EBAY.COM\",\"count\":2},{\"user\":\"b_pandaren_kwdm@APD.EBAY.COM\",\"count\":2},{\"user\":\"b_merch@CORP.EBAY.COM\",\"count\":1}],\"totalCount\":155},{\"opType\":\"getfileinfo\",\"topUsers\":[{\"user\":\"b_bis@CORP.EBAY.COM\",\"count\":78488},{\"user\":\"b_slng\",\"count\":47368},{\"user\":\"b_merch@CORP.EBAY.COM\",\"count\":19656},{\"user\":\"b_um@CORP.EBAY.COM\",\"count\":16164},{\"user\":\"b_sd_research@CORP.EBAY.COM\",\"count\":14070},{\"user\":\"jdidis@CORP.EBAY.COM\",\"count\":9475},{\"user\":\"b_sitespeed@CORP.EBAY.COM\",\"count\":7502},{\"user\":\"b_sscience@CORP.EBAY.COM\",\"count\":3253},{\"user\":\"b_ebayadvertising@CORP.EBAY.COM\",\"count\":2509},{\"user\":\"b_des@CORP.EBAY.COM\",\"count\":2168}],\"totalCount\":230470},{\"opType\":\"rename\",\"topUsers\":[{\"user\":\"b_sd_research@CORP.EBAY.COM\",\"count\":7758},{\"user\":\"b_merch@CORP.EBAY.COM\",\"count\":2990},{\"user\":\"b_bis@CORP.EBAY.COM\",\"count\":2715},{\"user\":\"b_sscience@CORP.EBAY.COM\",\"count\":1445},{\"user\":\"jdidis@CORP.EBAY.COM\",\"count\":1251},{\"user\":\"jguha@CORP.EBAY.COM\",\"count\":1094},{\"user\":\"b_um@CORP.EBAY.COM\",\"count\":487},{\"user\":\"b_des@CORP.EBAY.COM\",\"count\":326},{\"user\":\"b_sitespeed@CORP.EBAY.COM\",\"count\":242},{\"user\":\"b_find\",\"count\":157}],\"totalCount\":19562},{\"opType\":\"mkdirs\",\"topUsers\":[{\"user\":\"b_slng\",\"count\":49},{\"user\":\"b_des@CORP.EBAY.COM\",\"count\":39},{\"user\":\"b_ebayadvertising@CORP.EBAY.COM\",\"count\":12},{\"user\":\"b_bis@CORP.EBAY.COM\",\"count\":10},{\"user\":\"b_kylin@CORP.EBAY.COM\",\"count\":10},{\"user\":\"b_um@CORP.EBAY.COM\",\"count\":7},{\"user\":\"jguha@CORP.EBAY.COM\",\"count\":6},{\"user\":\"b_pandaren_kwdm@APD.EBAY.COM\",\"count\":5},{\"user\":\"hadoop/phxdpehdc9dn2103.stratus.phx.ebay.com@APD.EBAY.COM\",\"count\":4},{\"user\":\"chewu@CORP.EBAY.COM\",\"count\":4}],\"totalCount\":273},{\"opType\":\"setTimes\",\"topUsers\":[{\"user\":\"hadoop/phxaishdc9dn0778.phx.ebay.com@APD.EBAY.COM\",\"count\":13},{\"user\":\"hadoop/phxdpehdc9dn2103.stratus.phx.ebay.com@APD.EBAY.COM\",\"count\":13},{\"user\":\"hadoop/phxaishdc9dn0978.phx.ebay.com@APD.EBAY.COM\",\"count\":11},{\"user\":\"hadoop/phxaishdc9dn0834.phx.ebay.com@APD.EBAY.COM\",\"count\":11},{\"user\":\"hadoop/phxaishdc9dn0878.phx.ebay.com@APD.EBAY.COM\",\"count\":11},{\"user\":\"hadoop/phxaishdc9dn0578.phx.ebay.com@APD.EBAY.COM\",\"count\":10},{\"user\":\"hadoop/phxdpehdc9dn1980.stratus.phx.ebay.com@APD.EBAY.COM\",\"count\":10},{\"user\":\"hadoop/phxdpehdc9dn1978.stratus.phx.ebay.com@APD.EBAY.COM\",\"count\":10},{\"user\":\"hadoop/phxdpehdc9dn2005.stratus.phx.ebay.com@APD.EBAY.COM\",\"count\":10},{\"user\":\"hadoop/phxdpehdc9dn2090.stratus.phx.ebay.com@APD.EBAY.COM\",\"count\":9}],\"totalCount\":417},{\"opType\":\"open\",\"topUsers\":[{\"user\":\"b_bis@CORP.EBAY.COM\",\"count\":84441},{\"user\":\"b_um@CORP.EBAY.COM\",\"count\":54147},{\"user\":\"b_slng\",\"count\":46123},{\"user\":\"b_merch@CORP.EBAY.COM\",\"count\":13895},{\"user\":\"b_sitespeed@CORP.EBAY.COM\",\"count\":9020},{\"user\":\"jdidis@CORP.EBAY.COM\",\"count\":8684},{\"user\":\"b_kylin@CORP.EBAY.COM\",\"count\":3195},{\"user\":\"b_ebayadvertising@CORP.EBAY.COM\",\"count\":2487},{\"user\":\"b_des@CORP.EBAY.COM\",\"count\":1242},{\"user\":\"b_um\",\"count\":1055}],\"totalCount\":229836},{\"opType\":\"append\",\"topUsers\":[{\"user\":\"b_kylin@CORP.EBAY.COM\",\"count\":2}],\"totalCount\":2}],\"windowLenMs\":60000}]}";


        ObjectMapper objectMapper = new ObjectMapper();
        TopWindowResult result = null;
        try {
            result = objectMapper.readValue(data, TopWindowResult.class);
        } catch (IOException e) {
            e.printStackTrace();
        }
        //Assert.assertTrue(result.getWindows().getWindows().size() == 1);

        String data2 = "{\"timestamp\":\"2017-03-08T00:29:33-0700\",\"windows\":[{\"windowLenMs\":60000,\"ops\":[]},{\"windowLenMs\":300000,\"ops\":[]},{\"windowLenMs\":1500000,\"ops\":[]}]}";
        try {
            result = objectMapper.readValue(data2, TopWindowResult.class);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    @Test
    public void testTime() {
        String time = "2017-03-07T21:36:51-0700";
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ");

        try {
            long t1 = df.parse(time).getTime();
            String time2 = "2017-03-07 21:36:51";
            long t2 = DateTimeUtil.humanDateToSeconds(time2, TimeZone.getTimeZone("GMT-7")) * 1000;
            Assert.assertTrue(t1 == t2);
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }
}
