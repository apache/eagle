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

package org.apache.eagle.jpm.util;

import com.typesafe.config.Config;
import org.junit.Assert;
import org.junit.Test;


import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class JobNameNormalizationTest {


    @Test
    public void testNormalization() {
        Config config = mock(Config.class);
        when(config.hasPath("job.name.normalization.rules.key")).thenReturn(true);
        when(config.getString("job.name.normalization.rules.key")).thenReturn(Constants.JOB_NAME_NORMALIZATION_RULES_KEY_DEFAULT);
        JobNameNormalization jobNameNormalization = org.apache.eagle.jpm.util.JobNameNormalization.getInstance(config);
        Assert.assertEquals("oozie:launcher-shell-wf_co_xxx_for_xxx_v3-extract_org_data~", jobNameNormalization.normalize("oozie:launcher:T=shell:W=wf_co_xxx_for_xxx_v3:A=extract_org_data:ID=0002383-161115184801730-oozie-oozi-W"));
        Assert.assertEquals("[HQL]wireless_exclusive_products_sales", jobNameNormalization.normalize("[HQL]wireless_exclusive_products_sales"));
        Assert.assertEquals("insert overwrite table inter...a.xxx(Stage-3)", jobNameNormalization.normalize("insert overwrite table inter...a.xxx(Stage-3)"));
        Assert.assertEquals("uuid~", jobNameNormalization.normalize("cfc64640-f679-4d38-9d2c-b9c8310e4398"));
        Assert.assertEquals("~", jobNameNormalization.normalize("1111/11/11/11"));
        Assert.assertEquals("YHD~", jobNameNormalization.normalize("YHD1111/11/11/11"));
        Assert.assertEquals("YHD~", jobNameNormalization.normalize("YHD11111111/11_11"));
    }
}
