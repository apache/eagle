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
package org.apache.eagle.alert.engine.evaluator;

import org.apache.eagle.alert.engine.coordinator.PolicyDefinition;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Map;

/**
 * Created on 9/7/16.
 */
public class PoilcyExtendedTest {

    private static final ObjectMapper mapper = new ObjectMapper();

    @Test
    public void test() throws Exception {
        ArrayNode arrayNode = (ArrayNode)
                mapper.readTree(PoilcyExtendedTest.class.getResourceAsStream("/extend_policy.json"));
        Assert.assertEquals(1, arrayNode.size());
        for (JsonNode node : arrayNode) {
            PolicyDefinition definition = mapper.readValue(node, PolicyDefinition.class);

            Assert.assertNotNull(definition);
            Assert.assertNotNull(definition.getName());
            Assert.assertNotNull(definition.getDefinition());

            Assert.assertEquals(PolicyStreamHandlers.CUSTOMIZED_ENGINE, definition.getDefinition().getType());
            Assert.assertNotNull(definition.getDefinition().getProperties());

            Assert.assertTrue(definition.getDefinition().getProperties().containsKey("parentKey"));
            Map pkSetting = (Map) definition.getDefinition().getProperties().get("parentKey");
            Assert.assertTrue(pkSetting.containsKey("syslogStream"));

            Map syslogStreamSetting = (Map) pkSetting.get("syslogStream");
            Assert.assertTrue(syslogStreamSetting.containsKey("pattern"));
            Assert.assertEquals("%s-%s", syslogStreamSetting.get("pattern"));

            Assert.assertTrue(syslogStreamSetting.containsKey("columns"));
            Assert.assertEquals(3, ((List) syslogStreamSetting.get("columns")).size());

            break;
        }

    }

}
