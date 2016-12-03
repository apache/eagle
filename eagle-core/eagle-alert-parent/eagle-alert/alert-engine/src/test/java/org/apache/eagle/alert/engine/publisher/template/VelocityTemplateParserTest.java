/*
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

package org.apache.eagle.alert.engine.publisher.template;

import org.apache.velocity.exception.MethodInvocationException;
import org.apache.velocity.exception.ParseErrorException;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class VelocityTemplateParserTest {
    @Test
    public void testParseVelocityTemplate() {
        String templateString = "This alert ($category) was generated because $reason and $REASON from $source at $created_time";
        VelocityTemplateParser parser = new VelocityTemplateParser(templateString);
        Assert.assertEquals(5, parser.getReferenceNames().size());
        Assert.assertArrayEquals(new String[]{"category", "reason", "REASON", "source", "created_time"}, parser.getReferenceNames().toArray());
    }


    @Test(expected = ParseErrorException.class)
    public void testParseInvalidVelocityTemplate() {
        String templateString = "This alert ($category) was generated because $reason and $REASON from $source at $created_time #if() #fi";
        VelocityTemplateParser parser = new VelocityTemplateParser(templateString);
        Assert.assertEquals(5, parser.getReferenceNames().size());
        Assert.assertArrayEquals(new String[]{"category", "reason", "REASON", "source", "created_time"}, parser.getReferenceNames().toArray());
    }

    @Test
    public void testValidateVelocityContext() {
        String templateString = "This alert ($category) was generated because $reason and $REASON from $source at $created_time";
        VelocityTemplateParser parser = new VelocityTemplateParser(templateString);
        Map<String,Object> context = new HashMap<>();
        context.put("category", "UNKNOWN");
        context.put("reason", "timeout");
        context.put("REASON", "IO error");
        context.put("source","localhost");
        context.put("created_time", "2016-11-30 05:52:47,053");
        parser.validateContext(context);
    }

    @Test(expected = MethodInvocationException.class)
    public void testValidateInvalidVelocityContext() {
        String templateString = "This alert ($category) was generated because $reason and $REASON from $source at $created_time";
        VelocityTemplateParser parser = new VelocityTemplateParser(templateString);
        parser.validateContext(new HashMap<>());
    }
}
