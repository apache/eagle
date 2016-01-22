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

package org.apache.eagle.policy.siddhi;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.eagle.alert.entity.AlertDefinitionAPIEntity;
import org.apache.eagle.dataproc.core.JsonSerDeserUtils;
import org.apache.eagle.policy.PolicyManager;
import org.apache.eagle.policy.config.AbstractPolicyDefinition;
import org.apache.eagle.service.client.EagleServiceClientException;
import org.apache.eagle.service.client.EagleServiceConnector;
import org.apache.eagle.service.client.IEagleServiceClient;
import org.apache.eagle.service.client.impl.EagleServiceClientImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.siddhi.query.compiler.SiddhiCompiler;
import org.wso2.siddhi.query.compiler.exception.SiddhiParserException;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Utility class to check for Siddhi policy compilation validity.
 * Added as part of jira: EAGLE-95
 */
public class SiddhiPolicyEvaluatorUtility implements Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(SiddhiPolicyEvaluatorUtility.class);
    private Config config = ConfigFactory.load();

    /**
     * Check and update markdown status for the AlertDefinitionAPIEntity and update the markdown columns in the database.
     * @param entity
     * @throws SiddhiParserException
     */
    public void updateMarkdownDetails(AlertDefinitionAPIEntity entity) {
        if ("siddhiCEPEngine".equals(entity.getTags().get("policyType"))) {
            LOG.info("Checking the compilation status of the Siddhi query");
            AbstractPolicyDefinition abstractPolicyDef = null;

            try {
                abstractPolicyDef = JsonSerDeserUtils.deserialize(entity.getPolicyDef(), AbstractPolicyDefinition.class, PolicyManager.getInstance().getPolicyModules(entity.getTags().get("policyType")));
            } catch (Exception exception) {
                LOG.error("Exception in checking validity of the policy " + exception.getMessage() + ", policy will not be marked down by default");
            }

            try {
                checkQueryValidity(((SiddhiPolicyDefinition) abstractPolicyDef).getExpression());
                updatePolicyDetails(entity, false, null);
            } catch (SiddhiParserException exception) {
                updatePolicyDetails(entity, true, exception.getMessage());
            }
        }
    }

    /**
     * Check if the Siddhi query can compile successfully.
     * @param siddhiQuery
     */
    public void checkQueryValidity(String siddhiQuery) {
        try {
            // Checking for any errors during Siddhi query parsing
            SiddhiCompiler.parseQuery(siddhiQuery);
            LOG.info("Siddhi query: " + siddhiQuery + " validation: pass");
        } catch (SiddhiParserException exception) {
            LOG.error("Siddhi query: " + siddhiQuery + " validation: fail, reason being: " + exception.getMessage());
            throw exception;
        }
    }

    /**
     * Persists alert definition entity updated with markdown columns into HBase.
     * @param entity
     * @param markdownEnabled
     * @param markdownReason
     */
    private void updatePolicyDetails(AlertDefinitionAPIEntity entity, boolean markdownEnabled, String markdownReason) {
        List<AlertDefinitionAPIEntity> entityList = new ArrayList<>();
        entity.setMarkdownReason(null != markdownReason ? markdownReason : "");
        entity.setMarkdownEnabled(markdownEnabled);
        entityList.add(entity);

        EagleServiceConnector connector = new EagleServiceConnector(config);
        IEagleServiceClient client = new EagleServiceClientImpl(connector);

        try {
            client.create(entityList, "AlertDefinitionService");
        } catch (IOException | EagleServiceClientException exception) {
            LOG.error("Exception in updating markdown for policy in HBase", exception.getMessage());
        } finally {
            try {
                if (null != client)
                    client.close();
            } catch (IOException exception) {
                LOG.debug("Unable to close Eagle service client, " + exception.getMessage());
            }
        }
    }
}