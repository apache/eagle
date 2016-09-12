/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *    http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.eagle.alert.engine.topology;

import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;

/**
 * Storm 1.0.0 uses ByteBuffer, we need test it
 */
public class TestByteBuffer {
    @Test
    public void testBB() {
        ByteBuffer bb = ByteBuffer.allocate(100);
        bb.put((byte) 12);
        Assert.assertTrue(bb.hasArray());
        bb.rewind();
        Assert.assertEquals(12, bb.get());
    }

    @Test
    public void testMultipleStrings() throws Exception {
        ByteBuffer bb = ByteBuffer.allocate(100);
        bb.put("abc".getBytes("UTF-8"));
        bb.put("xyz".getBytes("UTF-8"));
        bb.put("1234".getBytes("UTF-8"));
        bb.rewind();
        if (bb.hasArray()) {
            int base = bb.arrayOffset();
            String ret = new String(bb.array(), base + bb.position(), bb.remaining());
            System.out.println("string is: " + ret);
        }
    }
}
