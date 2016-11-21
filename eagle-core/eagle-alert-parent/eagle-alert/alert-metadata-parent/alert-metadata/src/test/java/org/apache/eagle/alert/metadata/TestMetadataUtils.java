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
package org.apache.eagle.alert.metadata;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.eagle.alert.coordination.model.ScheduleState;
import org.apache.eagle.alert.coordination.model.internal.PolicyAssignment;
import org.apache.eagle.alert.engine.coordinator.StreamDefinition;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.sql.Connection;
import java.sql.DriverManager;

/**
 * Created by luokun on 2016/11/16.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(MetadataUtils.class)
public class TestMetadataUtils {

    @Rule
    public ExpectedException thrown= ExpectedException.none();

    @Test
    public void testGetKey() throws Exception {
        StreamDefinition stream = new StreamDefinition();
        Assert.assertNull(MetadataUtils.getKey(stream));
        PolicyAssignment policyAssignment = new PolicyAssignment();
        policyAssignment.setPolicyName("test");
        Assert.assertEquals("test", MetadataUtils.getKey(policyAssignment));
        ScheduleState scheduleState = new ScheduleState();
        scheduleState.setVersion("1.0");
        Assert.assertEquals("1.0", MetadataUtils.getKey(scheduleState));
    }

    @Test
    public void testGetKeyThrowable() {
        thrown.expect(RuntimeException.class);
        Object obj = new Object();
        MetadataUtils.getKey(obj);
    }

    /*
    @Test
    public void testGetJdbcConnection() throws Exception {
        System.setProperty("config.resource", "/application-mysql.conf");
        ConfigFactory.invalidateCaches();
        Config config = ConfigFactory.load().getConfig(MetadataUtils.META_DATA);
        Connection connection = null;
        PowerMockito.mockStatic(MetadataUtils.class);
        PowerMockito.when(MetadataUtils.getJdbcConnection(Mockito.any())).thenReturn(Mockito.mock(Connection.class));
        MetadataUtils.getJdbcConnection(config);
        Assert.assertNull(connection);
    }
    */

}