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
package org.apache.eagle.common.config;

import com.typesafe.config.ConfigException;
import com.typesafe.config.ConfigFactory;
import org.apache.commons.lang3.text.StrLookup;
import org.apache.commons.lang3.text.StrSubstitutor;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class TestConfigResolver {
    @Test
    public void testSimpleStringSubstitutor() {
        Map<String, String> valuesMap = new HashMap<>();
        valuesMap.put("animal", "quick brown fox");
        valuesMap.put("target", "lazy dog");
        String templateString = "The ${animal} jumped over the ${target}.";
        StrSubstitutor sub = new StrSubstitutor(valuesMap);
        String resolvedString = sub.replace(templateString);
        Assert.assertEquals("The quick brown fox jumped over the lazy dog.", resolvedString);
    }

    @Test
    public void testConfigStringSubstitutor() {
        String templateString = "The ${animal} jumped over the ${target}.";
        StrSubstitutor sub = new StrSubstitutor();
        sub.setVariableResolver(new StrLookup<Object>() {
            Map<String, String> valuesMap = new HashMap<String, String>() {{
                put("animal", "quick brown fox");
                put("target", "lazy dog");
            }};
            @Override
            public String lookup(String key) {
                return valuesMap.get(key);
            }
        });
        String resolvedString = sub.replace(templateString);
        Assert.assertEquals("The quick brown fox jumped over the lazy dog.", resolvedString);
    }

    @Test
    public void testConfigStringResolver(){
        ConfigStringResolver resolver = new ConfigStringResolver(ConfigFactory.parseMap(new HashMap<String,Object>(){{
            put("string.key","hello string");
            put("double.key",0.5);
            put("long.key",10000L);
            put("array.key", Arrays.toString(new String[] {"str1", "str2"}));
            put("object.key",new Pojo().toString());
        }}));
        Assert.assertEquals(
            "string.key=hello string,"
                + "double.key=0.5,"
                + "long.key=10000,"
                + "array.key=[str1, str2],"
                + "object.key=A pojo",
            resolver.resolve(
                "string.key=${string.key},"
                    + "double.key=${double.key},"
                    + "long.key=${long.key},"
                    + "array.key=${array.key},"
                    + "object.key=${object.key}"
            )
        );
    }

    @Test(expected = ConfigException.Missing.class)
    public void testConfigStringResolverException(){
        ConfigStringResolver resolver = new ConfigStringResolver(ConfigFactory.parseMap(new HashMap<String,Object>(){{
            put("exist.key","value");
        }}));
        resolver.resolve(
            "exist.key=${string.key},"
                + "notexist.key=${notexist.key}"
        );
    }

    private class Pojo {
        @Override
        public String toString() {
            return "A pojo";
        }
    }
}
