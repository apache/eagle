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
package org.apache.eagle.audit.entity;

import org.apache.eagle.audit.common.AuditConstants;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * @Since 11/14/16.
 */
public class TestGenericAuditEntity {

    @Test
    public void testEqualsObj() {
        GenericAuditEntity genericAuditEntity = new GenericAuditEntity();
        Assert.assertTrue(genericAuditEntity.equals(genericAuditEntity));
    }

    @Test
    public void testEqualsInstanceOf() {
        GenericAuditEntity genericAuditEntity = new GenericAuditEntity();
        Object obj = new Object();
        Assert.assertFalse(genericAuditEntity.equals(obj));
    }

    @Test
    public void testEqualsTrue() {
        GenericAuditEntity genericAuditEntity1 = new GenericAuditEntity();
        GenericAuditEntity genericAuditEntity2 = new GenericAuditEntity();
        Map<String, String> tags1 = new HashMap<>();
        tags1.put(AuditConstants.AUDIT_COLUMN_SERVICE_NAME, "testService");
        tags1.put(AuditConstants.AUDIT_COLUMN_USER_ID, "eagle1");
        tags1.put(AuditConstants.AUDIT_COLUMN_OPERATION, "UPDATE");
        tags1.put(AuditConstants.AUDIT_COLUMN_TIMESTAMP, "1479107183");
        Map<String, String> tags2 = new HashMap<>();
        tags2.put(AuditConstants.AUDIT_COLUMN_SERVICE_NAME, "testService");
        tags2.put(AuditConstants.AUDIT_COLUMN_USER_ID, "eagle1");
        tags2.put(AuditConstants.AUDIT_COLUMN_OPERATION, "UPDATE");
        tags2.put(AuditConstants.AUDIT_COLUMN_TIMESTAMP, "1479107183");
        genericAuditEntity1.setTags(tags1);
        genericAuditEntity2.setTags(tags2);
        Assert.assertTrue(genericAuditEntity1.equals(genericAuditEntity2));
    }

    @Test
    public void testEqualsFalse() {
        GenericAuditEntity genericAuditEntity1 = new GenericAuditEntity();
        GenericAuditEntity genericAuditEntity2 = new GenericAuditEntity();
        Map<String, String> tags1 = new HashMap<>();
        tags1.put(AuditConstants.AUDIT_COLUMN_SERVICE_NAME, "testService");
        tags1.put(AuditConstants.AUDIT_COLUMN_USER_ID, "eagle1");
        tags1.put(AuditConstants.AUDIT_COLUMN_OPERATION, "UPDATE");
        tags1.put(AuditConstants.AUDIT_COLUMN_TIMESTAMP, null);
        Map<String, String> tags2 = new HashMap<>();
        tags2.put(AuditConstants.AUDIT_COLUMN_SERVICE_NAME, "testService");
        tags2.put(AuditConstants.AUDIT_COLUMN_USER_ID, "eagle1");
        tags2.put(AuditConstants.AUDIT_COLUMN_OPERATION, "UPDATE");
        tags2.put(AuditConstants.AUDIT_COLUMN_TIMESTAMP, "1479107183");
        genericAuditEntity1.setTags(tags1);
        genericAuditEntity2.setTags(tags2);
        Assert.assertFalse(genericAuditEntity1.equals(genericAuditEntity2));
    }
}
