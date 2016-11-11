/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.eagle.common;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.io.Serializable;

@Ignore
public class TestSerializableUtils {
    @Test
    public void testSerializeObject() {
        SerializableUtils.ensureSerializable(0.5);
        byte[] bytes = SerializableUtils.serializeToByteArray(0.5);
        Assert.assertNotNull(bytes);
        Assert.assertEquals(0.5, SerializableUtils.deserializeFromByteArray(bytes, "0.5"));
    }

    @Test
    public void testSerializeObjectWithCompression() {
        SerializableUtils.ensureSerializable(0.5);
        byte[] bytes = SerializableUtils.serializeToCompressedByteArray(0.5);
        Assert.assertNotNull(bytes);
        Assert.assertEquals(0.5, SerializableUtils.deserializeFromCompressedByteArray(bytes, "0.5"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testUnserializableObject() {
        SerializableUtils.serializeToByteArray(new Object());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testEnsureUnserializableObject() {
        SerializableUtils.ensureSerializable(new UnserializablePOJO());
    }

    private class UnserializablePOJO implements Serializable {
        private Object obj;
    }
}
