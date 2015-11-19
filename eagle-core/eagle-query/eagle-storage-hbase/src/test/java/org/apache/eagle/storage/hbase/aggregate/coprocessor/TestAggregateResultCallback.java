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
package org.apache.eagle.storage.hbase.aggregate.coprocessor;

import org.apache.eagle.query.aggregate.AggregateFunctionType;
import org.apache.eagle.storage.hbase.query.coprocessor.AggregateResult;
import org.apache.eagle.storage.hbase.query.coprocessor.AggregateResultCallback;
import org.apache.eagle.storage.hbase.query.coprocessor.impl.AggregateResultCallbackImpl;
import org.apache.eagle.query.aggregate.raw.GroupbyKey;
import org.apache.eagle.query.aggregate.raw.GroupbyKeyValue;
import org.apache.eagle.query.aggregate.raw.GroupbyValue;
import org.apache.eagle.common.ByteUtil;
import junit.framework.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class TestAggregateResultCallback {
    @Test
    public void testUpdate(){
        // -----------------------------------------------------------------------------
        // key      |       max      min        count       avg         sum      | count
        // -----------------------------------------------------------------------------
        // a,b      |       1.0      2.0        3.0         4.0         5.0      | 3
        // a,b      |       2        3          6           5           6        | 6
        // a,b,c    |       3        3          5           5           6        | 5
        // a,b,c    |       4        5          5           5           7        | 5
        // -----------------------------------------------------------------------------
        // a,b      |       2        2          9           1           11       | 9
        // a,b,c    |       4        3          10          1           13       | 10
        // -----------------------------------------------------------------------------

        AggregateResultCallback callback = new AggregateResultCallbackImpl(Arrays.asList(
                        AggregateFunctionType.max,
                        AggregateFunctionType.min,
                        AggregateFunctionType.count,
                        AggregateFunctionType.avg,
                        AggregateFunctionType.sum));
        AggregateResult result1 = AggregateResult.build(
                Arrays.asList(
                    new String[]{"a","b"},
                    new String[]{"a","b"},
                    new String[]{"a","b","c"},
                    new String[]{"a","b","c"}
                ),
                Arrays.asList(
                    new double[]{1.0,2.0,3.0,4.0,5.0},
                    new double[]{2.0,3.0,6.0,5.0,6.0},
                    new double[]{3.0,3.0,5.0,5.0,6.0},
                    new double[]{4.0,5.0,5.0,5.0,7.0}
                ),
                Arrays.asList(3,6,5,5),
                System.currentTimeMillis(),
                System.currentTimeMillis()
        );
        callback.update(null,null,result1);
        AggregateResult callbackResult = callback.result();
        Assert.assertEquals(2,callbackResult.getKeyValues().size());

        // == ROW-#0 ==
        // Should be:
        // key      |       max      min        count       avg         sum      | count
        // -----------------------------------------------------------------------------
        // a,b,c    |       4        3          10          1           13       | 10
        GroupbyKeyValue row0 = callbackResult.getKeyValues().get(0);
//        Assert.assertEquals("a",new String(row0.getKey().getValue().get(0).copyBytes()));
//        Assert.assertEquals("b",new String(row0.getKey().getValue().get(1).copyBytes()));
        Assert.assertEquals(new GroupbyKey(Arrays.asList("a".getBytes(),"b".getBytes(),"c".getBytes())),row0.getKey());
        Assert.assertEquals(4.0,row0.getValue().get(0).get());
        Assert.assertEquals(10, ByteUtil.bytesToInt(row0.getValue().getMeta(0).getBytes()));
        Assert.assertEquals(3.0, row0.getValue().get(1).get());
        Assert.assertEquals(10, ByteUtil.bytesToInt(row0.getValue().getMeta(1).getBytes()));
        Assert.assertEquals(10.0,row0.getValue().get(2).get());
        Assert.assertEquals(10, ByteUtil.bytesToInt(row0.getValue().getMeta(2).getBytes()));
        Assert.assertEquals(1.0,row0.getValue().get(3).get());
        Assert.assertEquals(10, ByteUtil.bytesToInt(row0.getValue().getMeta(3).getBytes()));
        Assert.assertEquals(13.0,row0.getValue().get(4).get());
        Assert.assertEquals(10, ByteUtil.bytesToInt(row0.getValue().getMeta(4).getBytes()));

        // == ROW-#1 ==
        // Should be:
        // key      |       max      min        count       avg         sum      | count
        // -----------------------------------------------------------------------------
        // a,b      |       2        2          9           1           11       | 9
        GroupbyKeyValue row1 = callbackResult.getKeyValues().get(1);
        Assert.assertEquals(new GroupbyKey(Arrays.asList("a".getBytes(),"b".getBytes())),row1.getKey());
        Assert.assertEquals(2.0,row1.getValue().get(0).get());
        Assert.assertEquals(9, ByteUtil.bytesToInt(row1.getValue().getMeta(4).getBytes()));
        Assert.assertEquals(2.0, row1.getValue().get(1).get());
        Assert.assertEquals(9, ByteUtil.bytesToInt(row1.getValue().getMeta(4).getBytes()));
        Assert.assertEquals(9.0,row1.getValue().get(2).get());
        Assert.assertEquals(9, ByteUtil.bytesToInt(row1.getValue().getMeta(4).getBytes()));
        Assert.assertEquals(1.0,row1.getValue().get(3).get());
        Assert.assertEquals(9, ByteUtil.bytesToInt(row1.getValue().getMeta(4).getBytes()));
        Assert.assertEquals(11.0,row1.getValue().get(4).get());
        Assert.assertEquals(9, ByteUtil.bytesToInt(row1.getValue().getMeta(4).getBytes()));
    }

    @Test
    public void testAggregateResultTimestamp(){
        AggregateResult result1 = new AggregateResult();
        result1.setStartTimestamp(2l);
        result1.setStopTimestamp(4l);
        AggregateResult result2 = new AggregateResult();
        result2.setStartTimestamp(1l);
        result2.setStopTimestamp(3l);
        AggregateResultCallback  callback = new AggregateResultCallbackImpl(new ArrayList<AggregateFunctionType>());
        callback.update(null,null,result1);
        callback.update(null,null,result2);
        AggregateResult result3 = callback.result();
        Assert.assertEquals(1l,result3.getStartTimestamp());
        Assert.assertEquals(4l,result3.getStopTimestamp());
    }

    @Test
    public void testUpdatePerformance(){
        AggregateResultCallback callback = new AggregateResultCallbackImpl(
                Arrays.asList(
                        AggregateFunctionType.max,
                        AggregateFunctionType.min,
                        AggregateFunctionType.count,
                        AggregateFunctionType.avg));

        for(int i=0;i<1000000;i++) {
            AggregateResult result1 = new AggregateResult();
            result1.setStartTimestamp(System.currentTimeMillis());
            List<GroupbyKeyValue> keyValues = new ArrayList<GroupbyKeyValue>();

            // <a,b> - <1*3, 2*3, 3*3, 4*3>
            GroupbyKey key = new GroupbyKey();
            key.addValue("a".getBytes());
            key.addValue("b".getBytes());
            GroupbyValue value = new GroupbyValue();
            value.add(1.0);
            value.add(2.0);
            value.add(3.0);
            value.add(4.0);
            value.addMeta(3);
            value.addMeta(3);
            value.addMeta(3);
            value.addMeta(3);
            keyValues.add(new GroupbyKeyValue(key, value));

            // <a,b> - <1*3, 2*3, 3*3, 4*3>
            GroupbyKey key2 = new GroupbyKey();
            key2.addValue("a".getBytes());
            key2.addValue("b".getBytes());
            GroupbyValue value2 = new GroupbyValue();
            value2.add(2.0);
            value2.add(3.0);
            value2.add(4.0);
            value2.add(5.0);
            value2.addMeta(2);
            value2.addMeta(2);
            value2.addMeta(2);
            value2.addMeta(2);
            keyValues.add(new GroupbyKeyValue(key2, value2));
            result1.setKeyValues(keyValues);
            result1.setStopTimestamp(System.currentTimeMillis());
            callback.update(null, null, result1);
        }
        AggregateResult result2 = callback.result();
        Assert.assertNotNull(result2);
        Assert.assertTrue(result2.getStopTimestamp() > result2.getStartTimestamp());
        Assert.assertTrue(result2.getStartTimestamp() > 0);
        Assert.assertTrue(result2.getStopTimestamp() > 0);
    }
}
