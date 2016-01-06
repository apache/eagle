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
import junit.framework.Assert;
import org.apache.eagle.state.snapshot.StateSnapshotDAO;
import org.apache.eagle.state.snapshot.StateSnapshotEagleServiceDAOImpl;
import org.junit.Test;

/**
 * test state snapshot read/write
 */
public class TestStateSnapshotEagleServiceDAOImpl {
    @Test
    public void testReadWrite() throws Exception{
        System.setProperty("eagleProps.eagleService.host", "localhost");
        System.setProperty("eagleProps.eagleService.port", "38080");
        System.setProperty("eagleProps.eagleService.username", "admin");
        System.setProperty("eagleProps.eagleService.password", "secret");
        Config config = ConfigFactory.load();
        StateSnapshotDAO dao = new StateSnapshotEagleServiceDAOImpl(config);
        String stateString = "This is state snapshot";
        dao.writeState("site1", "applicationId1", "executorId1", stateString.getBytes("UTF-8"));
        byte[] state = dao.findLatestState("site1", "applicationId1", "executorId1").getState();
        String recoveredStateString = new String(state, "UTF-8");
        Assert.assertEquals(stateString, recoveredStateString);
    }
}
