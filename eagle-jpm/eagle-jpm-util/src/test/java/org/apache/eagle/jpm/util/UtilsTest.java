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

package org.apache.eagle.jpm.util;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class UtilsTest {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void testDateTimeToLong() {
        long timestamp = Utils.dateTimeToLong("2016-11-11T19:35:40.715GMT");
        Assert.assertEquals(1478892940715l, timestamp);

        timestamp = Utils.dateTimeToLong("2016-11-11T19:35:40.715GXMT");
        Assert.assertEquals(0l, timestamp);
    }

    @Test
    public void testParseMemory() {
        long mem = Utils.parseMemory("");
        Assert.assertEquals(0l, mem);
        mem = Utils.parseMemory("1g");
        Assert.assertEquals(1073741824l, mem);
        mem = Utils.parseMemory("1m");
        Assert.assertEquals(1048576l, mem);
        mem = Utils.parseMemory("1k");
        Assert.assertEquals(1024, mem);
        mem = Utils.parseMemory("1t");
        Assert.assertEquals(1099511627776l, mem);
        mem = Utils.parseMemory("1p");
        Assert.assertEquals(1125899906842624l, mem);
        mem = Utils.parseMemory("10000p");//overflow
        Assert.assertEquals(-7187745005283311616l, mem);
    }

    @Test
    public void testParseMemory1() {
        thrown.expect(IllegalArgumentException.class);
        Utils.parseMemory("0.1g");
    }

    @Test
    public void testMakeLockPath() {
        String lockpath = Utils.makeLockPath("/apps/mr/running/sitdId");
        Assert.assertEquals("/apps/mr/running/sitdid/locks", lockpath);
    }

    @Test
    public void testMakeLockPath1() {
        thrown.expect(IllegalArgumentException.class);
        Utils.makeLockPath("");
    }

    @Test
    public void testMakeLockPath2() {
        thrown.expect(IllegalArgumentException.class);
        Utils.makeLockPath(null);
    }

}
