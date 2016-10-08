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
package org.apache.alert.coordinator;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.eagle.alert.coordinator.impl.MetadataValdiator;
import org.apache.eagle.alert.coordinator.provider.InMemScheduleConext;
import org.junit.Test;

/**
 * Created on 10/2/16.
 */
public class TestMetadataValidator {

    private static final ObjectMapper om = new ObjectMapper();

    @Test
    public void validate() throws Exception {
        InMemScheduleConext context = new InMemScheduleConext();
        MetadataValdiator mv = new MetadataValdiator(context);


        // om.readValue(TestMetadataValidator.class.getResourceAsStream("/validation/datasources.json"), new Gene);
        // TODO add more test here.

    }
}
