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

import com.typesafe.config.Config;
import org.apache.eagle.alert.engine.publisher.AlertStreamFilter;
import org.apache.eagle.alert.engine.coordinator.PolicyDefinition;

import java.util.Collection;

/**
 * Alert Template Engine.
 */
public interface AlertTemplateEngine extends AlertStreamFilter {
    /**
     * Initialize AlertTemplateEngine with Config.
     */
    void init(Config config);

    /**
     * Register policy with definition.
     */
    void register(PolicyDefinition policyDefinition);

    /**
     * Register policy by policyId.
     */
    void unregister(String policyId);

    /**
     * @return registered policy definitions.
     */
    Collection<PolicyDefinition> getPolicies();
}