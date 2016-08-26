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

package org.apache.alert.coordinator;

import org.apache.eagle.alert.engine.coordinator.PolicyDefinition;
import org.apache.eagle.alert.utils.MapComparator;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.*;


public class DynamicPolicyLoaderTest {

    private PolicyDefinition def = new PolicyDefinition();
    private List<PolicyDefinition> current = new ArrayList<>();
    private Map<String, PolicyDefinition> cachedPolicies = new HashMap<>();
    private Map<String, PolicyDefinition> currPolicies = new HashMap<>();

    @Before
    public void setUp() {

        def.setName("test-policy-1");
        def.setDescription("test-policy-1-desc");
        current.add(def);
        current.forEach(pe -> currPolicies.put(pe.getName(), pe));

    }

    @Test
    public void testComparePolicys() {


        MapComparator<String, PolicyDefinition> comparator = new MapComparator<>(currPolicies, cachedPolicies);
        comparator.compare();
        Assert.assertTrue(comparator.getAddedKeys().size() == 1);
        Assert.assertTrue(comparator.getModifiedKeys().size() == 0);
        Assert.assertTrue(comparator.getRemovedKeys().size() == 0);
        Assert.assertEquals("test-policy-1", comparator.getAddedKeys().get(0));

        cachedPolicies = new HashMap<>(currPolicies);

        reset(current, currPolicies);

        PolicyDefinition def1 = new PolicyDefinition();
        def1.setName("test-policy-1");
        def1.setDescription("test1-policy-1-desc");
        current.add(def1);
        current.forEach(pe -> currPolicies.put(pe.getName(), pe));

        MapComparator<String, PolicyDefinition> comparator1 = new MapComparator<>(currPolicies, cachedPolicies);
        comparator1.compare();

        Assert.assertTrue(comparator1.getAddedKeys().size() == 0);
        Assert.assertTrue(comparator1.getModifiedKeys().size() == 1);
        Assert.assertTrue(comparator1.getRemovedKeys().size() == 0);
        Assert.assertEquals("test-policy-1", comparator1.getModifiedKeys().get(0));

        cachedPolicies = new HashMap<>(currPolicies);

        reset(current, currPolicies);

        current.forEach(pe -> currPolicies.put(pe.getName(), pe));

        MapComparator<String, PolicyDefinition> comparator2 = new MapComparator<>(currPolicies, cachedPolicies);
        comparator2.compare();

        Assert.assertTrue(comparator2.getAddedKeys().size() == 0);
        Assert.assertTrue(comparator2.getModifiedKeys().size() == 0);
        Assert.assertTrue(comparator2.getRemovedKeys().size() == 1);

        Assert.assertEquals("test-policy-1", comparator2.getRemovedKeys().get(0));


    }

    private void reset(List<PolicyDefinition> current, Map<String, PolicyDefinition> currPolicies) {
        current.clear();
        currPolicies.clear();
    }

}
