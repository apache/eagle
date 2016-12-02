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
package org.apache.eagle.metadata.model;
import org.junit.Assert;
import org.junit.Test;

import javax.xml.bind.JAXBException;
import java.io.InputStream;

/**
 * @Since 11/23/16.
 */
public class TestConfiguration {

    @Test
    public void testFromStream() throws JAXBException {
        InputStream inputStream = this.getClass().getResourceAsStream("/application-test.xml");
        Configuration configuration = Configuration.fromStream(inputStream);
        Assert.assertTrue(configuration.hasProperty("testkey1"));
    }

    @Test
    public void testFromResource() throws JAXBException {
        String resource = "application-test.xml";
        Configuration configuration = Configuration.fromResource(resource);
        Assert.assertEquals("testvalue1", configuration.getProperty("testkey1").getValue());
        Assert.assertEquals("testkey1", configuration.getProperty("testkey1").getName());
    }

    @Test
    public void testFromString() throws JAXBException {
        String xmlContext = "<configuration>\n" +
            "    <property>\n" +
            "        <name>testkey1</name>\n" +
            "        <value>testvalue1</value>\n" +
            "        <description>testdesc1</description>\n" +
            "    </property>\n" +
            "    <property>\n" +
            "        <name>testkey2</name>\n" +
            "        <value>testvalue2</value>\n" +
            "        <description>testdesc2</description>\n" +
            "    </property>\n" +
            "    <property>\n" +
            "        <name>testkey3</name>\n" +
            "        <value>testvalue3</value>\n" +
            "        <description>testdesc3</description>\n" +
            "    </property>\n" +
            "</configuration>";
        Configuration configuration = Configuration.fromString(xmlContext);
        Assert.assertEquals("testvalue1", configuration.getProperty("testkey1").getValue());
        Assert.assertEquals(3, configuration.size());
    }
}
