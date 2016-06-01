/**
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
package org.apache.eagle.alert.engine.sorter;

import org.junit.Assert;
import org.junit.Test;
import org.mapdb.BTreeMap;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;

public class MapDBTestSuite {
    @Test
    public void testOnHeapDB(){
        DB db = DBMaker.heapDB().make();
        BTreeMap<Long,String> map = db.treeMap("btree").keySerializer(Serializer.LONG).valueSerializer(Serializer.STRING).create();
        Assert.assertFalse(map.putIfAbsentBoolean(1L,"val_1"));
        Assert.assertTrue(map.putIfAbsentBoolean(1L,"val_2"));
        Assert.assertTrue(map.putIfAbsentBoolean(1L,"val_3"));
        Assert.assertFalse(map.putIfAbsentBoolean(2L,"val_4"));

        Assert.assertEquals("val_1",map.get(1L));
        Assert.assertEquals("val_4",map.get(2L));

        Assert.assertTrue(map.replace(2L,"val_4","val_5"));
        Assert.assertEquals("val_5",map.get(2L));

        map.close();
        db.close();
    }
}
