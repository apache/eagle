/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.eagle.metadata.service;

import org.apache.commons.collections.CollectionUtils;
import org.apache.eagle.alert.engine.coordinator.PolicyDefinition;
import org.apache.eagle.metadata.model.PolicyEntity;
import org.apache.eagle.metadata.service.memory.PolicyEntityServiceMemoryImpl;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

public class TestPolicyEntityServiceMemoryImpl {

    PolicyEntityService policyEntityService = new PolicyEntityServiceMemoryImpl();

    @Test
    public void test() {
        // define a prototype policy without site info
        PolicyDefinition policyDefinition = new PolicyDefinition();
        policyDefinition.setName("policy1");
        PolicyDefinition.Definition definition = new PolicyDefinition.Definition("siddhi",
                "from STREAM select * insert into out");
        policyDefinition.setDefinition(definition);
        policyDefinition.setInputStreams(Arrays.asList("STREAM"));
        policyDefinition.setOutputStreams(Arrays.asList("out"));
        // define publisher list
        List<String> alertPublisherIds = Arrays.asList("slack");

        PolicyEntity policyEntity = new PolicyEntity();
        policyEntity.setPolicyProto(policyDefinition);
        policyEntity.setAlertPublishmentIds(alertPublisherIds);
        PolicyEntity res = policyEntityService.createOrUpdatePolicyProto(policyEntity);
        Assert.assertTrue(res.getPolicyProto().equals(policyDefinition));
        Assert.assertTrue(CollectionUtils.isEqualCollection(res.getAlertPublishmentIds(), alertPublisherIds));

        Collection<PolicyEntity> policies =  policyEntityService.getAllPolicyProto();
        Assert.assertTrue(policies.size() == 1);

        PolicyEntity entity = policyEntityService.getPolicyProtoByUUID(policies.iterator().next().getUuid());
        Assert.assertTrue(entity.equals(policies.iterator().next()));

        // test update
        entity.getPolicyProto().setName("policy2");
        PolicyEntity updatedEntity = policyEntityService.update(entity);
        Assert.assertTrue(updatedEntity.getPolicyProto().getName().equals("policy2"));


        // test delete
        //Assert.assertTrue(policyEntityService.deletePolicyProtoByUUID(entity.getUuid()));
        policyEntityService.deletePolicyProtoByUUID(entity.getUuid());
        Assert.assertTrue(policyEntityService.getAllPolicyProto().size() == 0);
    }
}
