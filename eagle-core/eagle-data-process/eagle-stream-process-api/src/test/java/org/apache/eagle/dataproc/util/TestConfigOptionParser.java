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
package org.apache.eagle.dataproc.util;

import junit.framework.Assert;
import org.apache.commons.cli.ParseException;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * @since 8/23/15
 */
public class TestConfigOptionParser {
    private final static Logger LOG = LoggerFactory.getLogger(TestConfigOptionParser.class);

    @Test
    public void testValidCommandArguments() throws ParseException {
        String[] arguments = new String[]{
                "-D","key1=value1",
                "-D","key2=value2",
                "-D","key3=value3=something",
                "-D","key4=",
                "-D","key5=\"--param having whitespace\""
        };

        Map<String,String> config = new ConfigOptionParser().parseConfig(arguments);

        Assert.assertTrue(config.containsKey("key1"));
        Assert.assertTrue(config.containsKey("key2"));
        Assert.assertTrue(config.containsKey("key3"));
        Assert.assertTrue(config.containsKey("key4"));
        Assert.assertEquals("value1", config.get("key1"));
        Assert.assertEquals("value2", config.get("key2"));
        Assert.assertEquals("value3=something",config.get("key3"));
        Assert.assertEquals("",config.get("key4"));
        Assert.assertEquals("\"--param having whitespace",config.get("key5"));
    }

    @Test
    public void testValidCommandArgumentsAsSystem() throws ParseException {
        String[] arguments = new String[]{
                "-D","key1=value1",
                "-D","key2=value2",
                "-D","key3=value3=something",
                "-D","key4=",
        };

        new ConfigOptionParser().load(arguments);

        Assert.assertTrue(System.getProperties().containsKey("key1"));
        Assert.assertTrue(System.getProperties().containsKey("key2"));
        Assert.assertTrue(System.getProperties().containsKey("key3"));
        Assert.assertTrue(System.getProperties().containsKey("key4"));

        Assert.assertEquals("value1", System.getProperty("key1"));
        Assert.assertEquals("value2", System.getProperty("key2"));
        Assert.assertEquals("value3=something",System.getProperty("key3"));
        Assert.assertEquals("",System.getProperty("key4"));
    }

    @Test
    public void testInvalidCommandArgument1()  {
        String[] arguments = new String[]{
                "-D","key1"
        };

        try {
            new ConfigOptionParser().parseConfig(arguments);
            Assert.fail("Should throw ParseException");
        } catch (ParseException e) {
            LOG.info("Expected exception: " +e.getMessage());
        }
    }

    @Test
    public void testInvalidCommandArgument2()  {
        String[] arguments = new String[]{
                "-D","=value"
        };

        try {
            new ConfigOptionParser().parseConfig(arguments);
            Assert.fail("Should throw ParseException");
        } catch (ParseException e) {
            LOG.info("Expected exception: " + e.getMessage());
        }
    }
}