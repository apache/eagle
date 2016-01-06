/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *    http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.eagle.state.deltaevent;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.junit.Test;

/**
 * test kafka implementation of delta event DAO
 */
public class TestDeltaEventKafkaDAOImpl {
    @Test
    public void testWriteRead() throws Exception{
        Config config = ConfigFactory.load();
        String elementId = "executorId1_1";
        DeltaEventDAO deltaEventDAO = new DeltaEventKafkaDAOImpl(config, elementId);
        long startOffset = deltaEventDAO.write("abcefg1");
        deltaEventDAO.write("abcefg2");
        deltaEventDAO.write("abcefg3");
        deltaEventDAO.write("abcefg4");
        deltaEventDAO.load(startOffset, new DeltaEventReplayCallback() {
            @Override
            public void replay(Object event) {
                System.out.println(event);
            }
        });
    }
}
