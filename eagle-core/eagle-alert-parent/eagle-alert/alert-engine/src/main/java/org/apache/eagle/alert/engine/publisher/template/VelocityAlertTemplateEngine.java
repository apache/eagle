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
package org.apache.eagle.alert.engine.publisher.template;

import com.google.common.base.Preconditions;
import com.typesafe.config.Config;
import org.apache.eagle.alert.engine.coordinator.AlertTemplateDefinition;
import org.apache.eagle.alert.engine.coordinator.PolicyDefinition;
import org.apache.eagle.alert.engine.model.AlertStreamEvent;
import org.apache.eagle.common.DateTimeUtil;
import org.apache.velocity.Template;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.Velocity;
import org.apache.velocity.app.VelocityEngine;
import org.apache.velocity.runtime.RuntimeConstants;
import org.apache.velocity.runtime.resource.loader.StringResourceLoader;
import org.apache.velocity.runtime.resource.util.StringResourceRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class VelocityAlertTemplateEngine implements AlertTemplateEngine {
    private static final String ALERT_BODY_TPL_PREFIX = "AlertBodyTemplate";
    private static final String ALERT_SUBJECT_TPL_PREFIX = "AlertSubjectTemplate";
    private static final Logger LOG = LoggerFactory.getLogger(VelocityAlertTemplateEngine.class);
    private StringResourceRepository stringResourceRepository;
    private Map<String, PolicyDefinition> policyDefinitionRepository;
    private VelocityEngine engine;


    @Override
    public void init(Config config) {
        engine = new VelocityEngine();
        engine.setProperty(RuntimeConstants.RUNTIME_LOG_LOGSYSTEM_CLASS, "org.apache.velocity.runtime.log.Log4JLogChute");
        engine.setProperty("runtime.log.logsystem.log4j.logger", LOG.getName());
        engine.setProperty(Velocity.RESOURCE_LOADER, "string");
        engine.addProperty("string.resource.loader.class", StringResourceLoader.class.getName());
        engine.addProperty("string.resource.loader.repository.static", "false");
        engine.init();

        stringResourceRepository = (StringResourceRepository) engine.getApplicationAttribute(StringResourceLoader.REPOSITORY_NAME_DEFAULT);
        policyDefinitionRepository = new HashMap<>();
    }

    private String getAlertBodyTemplateName(String policyId) {
        return String.format("%s:%s", ALERT_BODY_TPL_PREFIX, policyId);
    }

    private String getAlertSubjectTemplateName(String policyId) {
        return String.format("%s:%s", ALERT_SUBJECT_TPL_PREFIX, policyId);
    }

    @Override
    public synchronized void register(PolicyDefinition policyDefinition) {
        LOG.info("Registering {}", policyDefinition.getName());
        Preconditions.checkNotNull(policyDefinition.getName(), "policyId is null");
        AlertTemplateDefinition alertTemplateDefinition = policyDefinition.getAlertTemplate();
        if (alertTemplateDefinition == null) {
            LOG.warn("Subject template of policy {} is null, using policy name by default");
            stringResourceRepository.putStringResource(getAlertSubjectTemplateName(policyDefinition.getName()), policyDefinition.getName());

            LOG.warn("Body template of policy {} is null, using $ALERT_EVENT by default");
            String defaultAlertBodyTmpl = String.format("Message: $%s (Auto-generated alert message as template not defined in policy %s)",
                AlertContextFields.ALERT_EVENT, policyDefinition.getName());
            stringResourceRepository.putStringResource(getAlertBodyTemplateName(policyDefinition.getName()), defaultAlertBodyTmpl);
        } else if (alertTemplateDefinition.getType().equals(AlertTemplateDefinition.TemplateType.TEXT)) {
            if (alertTemplateDefinition.getSubjectTemplate() != null) {
                stringResourceRepository.putStringResource(getAlertSubjectTemplateName(policyDefinition.getName()), alertTemplateDefinition.getSubjectTemplate());
            } else {
                LOG.warn("Subject template of policy {} is null, using policy name by default");
                stringResourceRepository.putStringResource(getAlertSubjectTemplateName(policyDefinition.getName()), policyDefinition.getName());
            }
            if (alertTemplateDefinition.getBodyTemplate() != null) {
                stringResourceRepository.putStringResource(getAlertBodyTemplateName(policyDefinition.getName()), alertTemplateDefinition.getBodyTemplate());
            } else {
                LOG.warn("Body template of policy {} is null, using ALERT_EVENT by default");
                stringResourceRepository.putStringResource(getAlertBodyTemplateName(policyDefinition.getName()), "$" + AlertContextFields.ALERT_EVENT);
            }
        } else {
            throw new IllegalArgumentException("Unsupported alert template type " + alertTemplateDefinition.getType());
        }
        policyDefinitionRepository.put(policyDefinition.getName(), policyDefinition);
    }

    @Override
    public synchronized void unregister(String policyId) {
        LOG.info("Unregistering {}", policyId);
        stringResourceRepository.removeStringResource(getAlertBodyTemplateName(policyId));
        stringResourceRepository.removeStringResource(getAlertSubjectTemplateName(policyId));
        policyDefinitionRepository.remove(policyId);
    }

    @Override
    public synchronized AlertStreamEvent filter(AlertStreamEvent event) {
        Preconditions.checkArgument(this.policyDefinitionRepository.containsKey(event.getPolicyId()), "Unknown policyId " + event.getPolicyId());
        PolicyDefinition policyDefinition = this.policyDefinitionRepository.get(event.getPolicyId());
        StringWriter bodyWriter = new StringWriter();
        StringWriter subjectWriter = new StringWriter();
        try {
            VelocityContext alertContext = buildAlertContext(policyDefinition, event);
            Template template = engine.getTemplate(getAlertBodyTemplateName(event.getPolicyId()));
            template.merge(alertContext, bodyWriter);
            event.setBody(bodyWriter.toString());

            template = engine.getTemplate(getAlertSubjectTemplateName(event.getPolicyId()));
            template.merge(alertContext, subjectWriter);
            event.setSubject(subjectWriter.toString());
        } finally {
            try {
                bodyWriter.close();
            } catch (IOException e) {
                LOG.warn(e.getMessage(), e);
            }
            try {
                subjectWriter.close();
            } catch (IOException e) {
                LOG.warn(e.getMessage(), e);
            }
        }
        return event;
    }

    @Override
    public synchronized Collection<PolicyDefinition> getPolicies() {
        return policyDefinitionRepository.values();
    }

    private static VelocityContext buildAlertContext(PolicyDefinition policyDefinition, AlertStreamEvent event) {
        VelocityContext context = new VelocityContext();
        context.put(AlertContextFields.STREAM_ID, event.getStreamId());
        context.put(AlertContextFields.ALERT_ID, event.getAlertId());
        context.put(AlertContextFields.CREATED_BY, event.getCreatedBy());
        context.put(AlertContextFields.CREATED_TIMESTAMP, event.getCreatedTime());
        context.put(AlertContextFields.CREATED_TIME, DateTimeUtil.millisecondsToHumanDateWithSeconds(event.getCreatedTime()));
        context.put(AlertContextFields.ALERT_TIMESTAMP, event.getTimestamp());
        context.put(AlertContextFields.ALERT_TIME, DateTimeUtil.millisecondsToHumanDateWithSeconds(event.getTimestamp()));
        context.put(AlertContextFields.ALERT_SCHEMA, event.getSchema());
        context.put(AlertContextFields.ALERT_EVENT, event);

        context.put(AlertContextFields.POLICY_ID, policyDefinition.getName());
        context.put(AlertContextFields.POLICY_DESC, policyDefinition.getDescription());
        context.put(AlertContextFields.POLICY_TYPE, policyDefinition.getDefinition().getType());
        context.put(AlertContextFields.POLICY_DEFINITION, policyDefinition.getDefinition().getValue());
        context.put(AlertContextFields.POLICY_HANDLER, policyDefinition.getDefinition().getHandlerClass());

        for (Map.Entry<String, Object> entry : event.getDataMap().entrySet()) {
            context.put(entry.getKey(), entry.getValue());
        }
        return context;
    }
}