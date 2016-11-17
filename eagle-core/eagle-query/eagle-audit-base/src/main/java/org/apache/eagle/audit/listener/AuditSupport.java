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

import org.apache.eagle.audit.common.AuditEvent;
import org.apache.eagle.audit.common.AuditListenerMap;
import org.apache.eagle.audit.entity.GenericAuditEntity;

import java.io.Serializable;
import java.util.List;

public class AuditSupport implements Serializable {

    private AuditListenerMap map = new AuditListenerMap();

    public AuditSupport(Object sourceBean) {
        if (sourceBean == null) {
            throw new NullPointerException();
        }
        source = sourceBean;
    }

    public void addAuditListener(AuditListener listener) {
        if (listener == null) {
            return;
        }
        if (listener instanceof AuditListenerProxy) {
            AuditListenerProxy proxy = (AuditListenerProxy) listener;
            addAuditListener(proxy.getPropertyName(), proxy.getListener());
        } else {
            this.map.add(null, listener);
        }
    }

    public void addAuditListener(String propertyName, AuditListener listener) {
        if (listener == null || propertyName == null) {
            return;
        }
        listener = this.map.extract(listener);
        if (listener != null) {
            this.map.add(propertyName, listener);
        }
    }

    public void removeAuditListener(AuditListener listener) {
        if (listener == null) {
            return;
        }
        if (listener instanceof AuditListenerProxy) {
            AuditListenerProxy proxy = (AuditListenerProxy) listener;
            removeAuditListener(proxy.getPropertyName(), proxy.getListener());
        } else {
            this.map.remove(null, listener);
        }
    }

    public void removeAuditListener(String propertyName, AuditListener listener) {
        if (listener == null || propertyName == null) {
            return;
        }
        listener = this.map.extract(listener);
        if (listener != null) {
            this.map.remove(propertyName, listener);
        }
    }

    public void fireAudit(String serviceName, List<GenericAuditEntity> auditEntities) {
        if (null != serviceName && null != auditEntities && 0 != auditEntities.size()) {
            fireAudit(new AuditEvent(this.source, serviceName, auditEntities));
        }
    }

    public void fireAudit(AuditEvent event) {
        if (null != event.getAuditEntities() && 0 != event.getAuditEntities().size()) {
            AuditListener[] common = this.map.get(null);
            AuditListener[] named = (null != event.getServiceName()) ? this.map.get(event.getServiceName()) : null;
            fire(common, event);
            fire(named, event);
        }
    }

    private static void fire(AuditListener[] listeners, AuditEvent event) {
        if (listeners != null) {
            for (AuditListener listener : listeners) {
                listener.auditEvent(event);
            }
        }
    }

    public AuditListener[] getAuditListeners() {
        return this.map.getListeners();
    }

    public AuditListener[] getAuditListeners(String propertyName) {
        return this.map.getListeners(propertyName);
    }

    public boolean hasListeners(String propertyName) {
        return this.map.hasListeners(propertyName);
    }

    private Object source;
}