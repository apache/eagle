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
package org.apache.eagle.audit.listener;

import junit.framework.Assert;
import org.apache.eagle.audit.common.AuditListenerMap;
import org.apache.eagle.audit.entity.GenericAuditEntity;
import org.junit.Test;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * @Since 11/9/16.
 */
public class TestAuditSupport {

    @Test
    public void addAuditLFieldListenerNull() throws Exception {
        AuditSupport auditSupport = new AuditSupport(this);
        Field mapField = AuditSupport.class.getDeclaredField("map");
        mapField.setAccessible(true);
        AuditListenerMap map = (AuditListenerMap)mapField.get(auditSupport);
        AuditListener auditListener = null;
        auditSupport.addAuditListener(auditListener);
        Assert.assertTrue(map.getEntries() == Collections.<Map.Entry<String, AuditListener[]>>emptySet());
    }

    @Test
    public void addAuditProxyNullListener() throws Exception {
        AuditSupport auditSupport = new AuditSupport(this);
        Field mapField = AuditSupport.class.getDeclaredField("map");
        mapField.setAccessible(true);
        AuditListenerMap map = (AuditListenerMap)mapField.get(auditSupport);
        AuditListener auditListener = new AuditListenerProxy("test", null);
        auditSupport.addAuditListener(auditListener);
        Assert.assertNull(map.get("test"));
    }

    @Test
    public void addAuditProxyHbase() throws Exception {
        AuditSupport auditSupport = new AuditSupport(this);
        Field mapField = AuditSupport.class.getDeclaredField("map");
        mapField.setAccessible(true);
        AuditListenerMap map = (AuditListenerMap)mapField.get(auditSupport);
        AuditListener auditListener = new AuditListenerTestHelper();
        auditSupport.addAuditListener(auditListener);
        Assert.assertNull(map.get("test"));
    }

    @Test
    public void testRemoveNull() {
        AuditSupport auditSupport = new AuditSupport(this);
        AuditListener auditListener = null;
        auditSupport.removeAuditListener(auditListener);
    }

    @Test
    public void testRemoveAuditProxy() {
        AuditSupport auditSupport = new AuditSupport(this);
        AuditListener auditListener = new AuditListenerTestHelper();
        auditSupport.addAuditListener(auditListener);
        auditSupport.removeAuditListener(auditListener);
    }

    @Test
    public void testFireAudit() throws Exception {
        AuditSupport auditSupport = new AuditSupport(this);
        Field mapField = AuditSupport.class.getDeclaredField("map");
        mapField.setAccessible(true);
        AuditListenerMap map = (AuditListenerMap)mapField.get(auditSupport);
        AuditListener auditListener1 = new AuditListenerTestHelper();
        AuditListener auditListener2 = new AuditListenerTestHelper();
        AuditListener[] listeners = {auditListener1, auditListener2};
        map.set("testServiceName", listeners);
        map.set(null, listeners);
        List<GenericAuditEntity> auditEntities = new ArrayList<>();
        GenericAuditEntity genericAuditEntity = new GenericAuditEntity();
        auditEntities.add(genericAuditEntity);
        auditSupport.fireAudit("testServiceName", auditEntities);
    }
}
