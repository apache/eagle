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
package org.apache.eagle.correlation.meta;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.eagle.alert.engine.coordinator.StreamDefinition;
import org.junit.Test;

/**
 * Created on 3/11/16.
 */
public class JsonTest {

    @Test
    public void streamDefTest() throws Exception {

        ObjectMapper mapper = new ObjectMapper();
        mapper.readValue(JsonTest.class.getResourceAsStream("/streamDef.json"), StreamDefinition.class);

        com.fasterxml.jackson.databind.ObjectMapper mapper2 = new com.fasterxml.jackson.databind.ObjectMapper();
        mapper2.readValue(JsonTest.class.getResourceAsStream("/streamDef.json"), StreamDefinition.class);

    }

}
