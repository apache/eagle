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
package org.apache.eagle.alert.engine.utils;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class CompressionUtilsTest {
    private final static Logger LOG = LoggerFactory.getLogger(CompressionUtilsTest.class);

    @Test
    public void testCompressAndDecompress() throws IOException {
        String value = "http://www.apache.org/licenses/LICENSE-2.0";
        byte[] original = value.getBytes();
        byte[] compressed = CompressionUtils.compress(original);
        byte[] decompressed = CompressionUtils.decompress(compressed);

        LOG.info("original size: {}",original.length);
        LOG.info("compressed size: {}",compressed.length);
        LOG.info("decompressed size: {}",decompressed.length);

        String decompressedValue = new String(decompressed);
        Assert.assertEquals(value,decompressedValue);
    }
}
