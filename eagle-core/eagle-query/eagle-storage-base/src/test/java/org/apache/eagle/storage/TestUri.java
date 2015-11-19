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
package org.apache.eagle.storage;

import junit.framework.Assert;
import org.junit.Test;

import java.net.URI;

/**
 * @since 3/23/15
 */
public class TestUri {
    @Test
    public void testUri(){
        String url = "eagle:hbase://zk1:2181,zk2:2181/hbase?connectionTimeout=12";
        String cleanURI = url.substring(6);

        URI uri = URI.create(cleanURI);
        Assert.assertEquals("hbase",uri.getScheme());

        // the problem is here, can not parse host and port
        Assert.assertEquals(null,uri.getHost());

        Assert.assertEquals(-1,uri.getPort());
        Assert.assertEquals("/hbase",uri.getPath());
    }
}
